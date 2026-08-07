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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

/**
 * Regression test for Parquet statistics encoding of DECIMAL columns written by Prestissimo.
 *
 * <p>When a DECIMAL(p, s) column is stored as a Parquet INT32 or INT64 physical type (i.e.
 * precision ≤ 18 with storeDecimalAsInteger enabled), the Iceberg lower/upper bounds must be
 * written as big-endian two's-complement bytes matching the unscaled integer value. Previously,
 * the bounds were written in little-endian byte order, causing MIN/MAX aggregations that rely on
 * Iceberg file statistics to return corrupted values (e.g. 2709913.60 instead of 100.00).
 */
public class TestIcebergDecimalStats
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner javaQueryRunner = javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
        NativeQueryRunnerUtils.createAllIcebergTables(javaQueryRunner);
        return javaQueryRunner;
    }

    @Test
    public void testDecimalMinMaxWithParquetStats()
    {
        String tableName = "iceberg.tpch.test_decimal_stats";
        try {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("CREATE TABLE " + tableName + " (id INT, amount DECIMAL(10,2))");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 100.00), (2, 600.00)", 2);

            assertQuery(
                    "SELECT MIN(amount), MAX(amount) FROM " + tableName,
                    "VALUES (CAST(100.00 AS DECIMAL(10,2)), CAST(600.00 AS DECIMAL(10,2)))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}

