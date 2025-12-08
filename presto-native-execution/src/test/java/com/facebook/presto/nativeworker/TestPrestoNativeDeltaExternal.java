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

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveQueryRunner.getFileHiveMetastore;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.PRINCIPAL_PRIVILEGES;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.createHiveSymlinkTable;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.createSchemaIfNotExist;
import static java.util.Collections.emptyList;

@Test(groups = {"parquet"})
public class TestPrestoNativeDeltaExternal
        extends AbstractTestQueryFramework
{
    private final String storageFormat = "PARQUET";
    private static final String TEST_TABLE = "test_delta_external";
    private static final String TEST_SCHEMA = "delta_test_schema";
    private static final String DELTA_TABLE_RESOURCE_PATH = "deltatables/my_delta_table";

    @Override
    protected void createTables()
    {
        super.createTables();

        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        ExtendedHiveMetastore metastore = getFileHiveMetastore((DistributedQueryRunner) queryRunner);

        // Get the absolute path to the Delta table manifest in test resources
        URL resourceUrl = Resources.getResource(DELTA_TABLE_RESOURCE_PATH);
        String deltaTablePath = new File(resourceUrl.getPath()).getAbsolutePath();
        String manifestPath = deltaTablePath + "/_symlink_format_manifest";

        // Define columns matching the Delta table schema
        ImmutableList<Column> columns = ImmutableList.of(
                new Column("id", HiveType.HIVE_LONG, Optional.empty(), Optional.empty()),
                new Column("name", HiveType.HIVE_STRING, Optional.empty(), Optional.empty()),
                new Column("age", HiveType.HIVE_LONG, Optional.empty(), Optional.empty()));

        // Create schema if it doesn't exist
        createSchemaIfNotExist(queryRunner, TEST_SCHEMA);

        // Create external table pointing to Delta manifest
        if (!metastore.getTable(METASTORE_CONTEXT, TEST_SCHEMA, TEST_TABLE).isPresent()) {
            metastore.createTable(
                    METASTORE_CONTEXT,
                    createHiveSymlinkTable(TEST_SCHEMA, TEST_TABLE, columns, manifestPath),
                    PRINCIPAL_PRIVILEGES,
                    emptyList());
        }
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testQueryDeltaExternalTable()
    {
        assertQuery(
                "SELECT * FROM " + TEST_SCHEMA + "." + TEST_TABLE + " ORDER BY id",
                "VALUES (BIGINT '1', 'Alice', BIGINT '34'), (BIGINT '2', 'Bob', BIGINT '28'), (BIGINT '3', 'Charlie', BIGINT '42')");
    }
}
