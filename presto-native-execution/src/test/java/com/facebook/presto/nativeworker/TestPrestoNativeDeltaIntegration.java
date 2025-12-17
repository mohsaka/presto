/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.delta.AbstractDeltaDistributedQueryTestBase;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.DELTA_DEFAULT_STORAGE_FORMAT;
import static java.lang.String.format;

public class TestPrestoNativeDeltaIntegration
        extends AbstractDeltaDistributedQueryTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeDeltaQueryRunnerBuilder().
                setExtraProperties(ImmutableMap.of(
                "experimental.pushdown-subfields-enabled", "true",
                "experimental.pushdown-dereference-enabled", "true")).build();

        // Create the test Delta tables in HMS
        for (String deltaTestTable : DELTA_TEST_TABLE_LIST) {
            registerDeltaTableInHMS(queryRunner, deltaTestTable, deltaTestTable);
        }

        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaDeltaQueryRunnerBuilder()
                .setStorageFormat(DELTA_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readPrimitiveTypeData(String version)
    {
        // Test reading following primitive types from a Delta table
        // (all integers, float, double, decimal, boolean, varchar, varbinary)
        String testQuery =
                format("SELECT * FROM \"%s\".\"%s\"", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        "data-reader-primitives"));
        String expResultsQuery = getPrimitiveTypeTableData();
        assertQuery(testQuery, expResultsQuery);
    }

    /**
     * Expected results for table "data-reader-primitives"
     */
    private static String getPrimitiveTypeTableData()
    {
        // Create query for the expected results.
        List<String> expRows = new ArrayList<>();
        for (byte i = 0; i < 10; i++) {
            expRows.add(format("SELECT " +
                    "   cast(%s as integer)," +
                    "   cast(%s as bigint)," +
                    "   cast(%s as tinyint)," +
                    "   cast(%s as smallint)," +
                    "   %s," +
                    "   cast(%s as real)," +
                    "   cast(%s as double), " +
                    "   '%s', " +
                    "   cast(X'0%s0%s' as varbinary), " +
                    "   cast(%s as decimal)", i, i, i, i, (i % 2 == 0 ? "true" : "false"), i, i, i, i, i, i));
        }
        expRows.add("SELECT null, null, null, null, null, null, null, null, null, null");
        return Joiner.on(" UNION ").join(expRows);
    }
}
