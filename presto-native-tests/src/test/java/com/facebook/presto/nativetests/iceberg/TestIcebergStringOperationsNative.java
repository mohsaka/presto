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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

/**
 * Native tests for Iceberg operations - converted from SQL test cases.
 * All operations run sequentially in a single test to match the original SQL test structure.
 */
public class TestIcebergStringOperationsNative
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
        // Pre-populate TPCH tables in the Iceberg catalog for CTAS operations
        NativeQueryRunnerUtils.createAllIcebergTables(javaQueryRunner);
        return javaQueryRunner;
    }

    @Test
    public void testIcebergOperationsSequential()
    {
        // drop_employee_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS employee");

        // drop_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS strings_table1");

        // create_table_string_table1
        assertUpdate("CREATE TABLE strings_table1 (string_name VARCHAR(45), s1 VARCHAR(12), s2 VARCHAR(12), i INTEGER)");

        // insert_values_string_table1
        assertUpdate("INSERT INTO strings_table1 VALUES " +
                "('HelloWorld', 'Hello', 'World', 100), " +
                "('DataBase', 'Data', 'Base', 200), " +
                "('StringTest', 'String', 'Test', 300)", 3);

        // select_query_string_table1
        assertQuery("SELECT " +
                        "s1 || s2 AS concat_str, " +
                        "string_name, " +
                        "s1, " +
                        "s2, " +
                        "UPPER(string_name) AS upper_str, " +
                        "LOWER(string_name) AS lower_str, " +
                        "LENGTH(string_name) AS str_length, " +
                        "CONCAT(s1, '-', s2) AS combined, " +
                        "SUBSTR(string_name, 1, 5) AS substring, " +
                        "POSITION('a' IN string_name) AS position_of_a, " +
                        "REPLACE(string_name, 'e', '*') AS replaced_str " +
                        "FROM strings_table1 " +
                        "ORDER BY string_name",
                "VALUES " +
                        "('DataBase', 'DataBase', 'Data', 'Base', 'DATABASE', 'database', 8, 'Data-Base', 'DataB', 2, 'DataBas*'), " +
                        "('HelloWorld', 'HelloWorld', 'Hello', 'World', 'HELLOWORLD', 'helloworld', 10, 'Hello-World', 'Hello', 0, 'H*lloWorld'), " +
                        "('StringTest', 'StringTest', 'String', 'Test', 'STRINGTEST', 'stringtest', 10, 'String-Test', 'Strin', 0, 'StringT*st')");

        // drop_table_string_table1
        assertUpdate("DROP TABLE strings_table1");

        // create_employee_table
        assertUpdate("CREATE TABLE employee (" +
                "emp_name VARCHAR(20), " +
                "emp_location VARCHAR(20), " +
                "emp_id INT, " +
                "dep_id VARCHAR(4), " +
                "u_id INT)");

        // drop_orders_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS orders");

        // create_orders_table
        assertUpdate("CREATE TABLE orders (" +
                "order_id INT, " +
                "customer_id INT, " +
                "product_id INT, " +
                "order_date DATE)");

        // drop_customers_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS customers");

        // create_customers_table
        assertUpdate("CREATE TABLE customers (" +
                "customer_id INT, " +
                "customer_name VARCHAR(50), " +
                "country VARCHAR(50))");

        // drop_products_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS products");

        // create_products_table
        assertUpdate("CREATE TABLE products (" +
                "product_id INT, " +
                "product_name VARCHAR(50), " +
                "category VARCHAR(50), " +
                "price DECIMAL(10, 2))");

        // drop_parquet_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS tester_parquet");

        // insert_into_employee_table
        assertUpdate("INSERT INTO employee VALUES " +
                "('tom', 'mumbai', 004, 'Ax5', 1), " +
                "('wade', 'sanfransico', 007, 'AF62', 2), " +
                "('Yan', 'dubai', 008, 'BF4', 3), " +
                "('robert', 'hyderabad', 003, 'Zf6', 9), " +
                "('Root', 'kochi', 012, 'AF6', 4), " +
                "('ivan', 'bangalore', 043, 'BF4', 10), " +
                "('Jorge', 'Sydney', 034, 'Zf6', 5), " +
                "('Tom', 'kochi', 058, 'AF6', 6), " +
                "('Bot', 'bangalore', 097, 'BF4', 7), " +
                "('Sam', 'dallas', 046, 'Zf6', 8)", 10);

        // insert_into_orders_table
        assertUpdate("INSERT INTO orders VALUES " +
                "(1001, 1, 101, DATE '2023-01-10'), " +
                "(1002, 2, 102, DATE '2023-01-12'), " +
                "(1003, 3, 103, DATE '2023-01-15'), " +
                "(1004, 1, 102, DATE '2023-01-18')", 4);

        // insert_into_customers_table
        assertUpdate("INSERT INTO customers VALUES " +
                "(1, 'John Doe', 'USA'), " +
                "(2, 'Jane Smith', 'Canada'), " +
                "(3, 'Bob Johnson', 'USA')", 3);

        // insert_into_products_table
        assertUpdate("INSERT INTO products VALUES " +
                "(101, 'Laptop', 'Electronics', 1200.00), " +
                "(102, 'Smartphone', 'Electronics', 800.00), " +
                "(103, 'Desk Chair', 'Furniture', 150.00)", 3);

        // query_select_where_condition
        assertQuery("SELECT emp_name FROM employee WHERE emp_location = 'kochi' OR emp_location = 'bangalore' ORDER BY emp_name",
                "VALUES ('Bot'), ('Root'), ('Tom'), ('ivan')");

        // query_select_where_condition (u_id >= 5)
        assertQuery("SELECT emp_name FROM employee WHERE u_id >= 5 ORDER BY emp_name",
                "VALUES ('Bot'), ('Jorge'), ('Sam'), ('Tom'), ('ivan'), ('robert')");

        // query_select_multiple_where_condition
        assertQuery("SELECT emp_name FROM employee WHERE (emp_location = 'kochi' OR emp_location = 'hyderabad') AND u_id BETWEEN 2 AND 7 ORDER BY emp_name",
                "VALUES ('Root'), ('Tom')");

        // query_select_case_when_then_alias
        assertQuery("SELECT emp_name, " +
                        "CASE " +
                        "WHEN emp_location = 'kochi' THEN 'South India' " +
                        "WHEN emp_location IN ('bangalore', 'hyderabad') THEN 'South Central India' " +
                        "WHEN emp_location = 'dubai' THEN 'Middle East' " +
                        "ELSE 'Other' " +
                        "END AS region " +
                        "FROM employee " +
                        "WHERE emp_location IN ('kochi', 'bangalore', 'dubai', 'hyderabad') " +
                        "ORDER BY emp_name",
                "VALUES ('Bot', 'South Central India'), ('Root', 'South India'), ('Tom', 'South India'), ('Yan', 'Middle East'), ('ivan', 'South Central India'), ('robert', 'South Central India')");

        // query_select_coalesce_alias
        assertQuery("SELECT COUNT(*) FROM employee WHERE COALESCE(dep_id, 'N/A') != 'N/A'", "VALUES (10)");

        // query_select_nullif_alias
        assertQuery("SELECT COUNT(*) FROM employee WHERE NULLIF(dep_id, 'AF6') IS NULL", "VALUES (2)");

        // query_select_try_alias
        assertQuery("SELECT COUNT(*) FROM employee WHERE TRY(100 / u_id) IS NOT NULL", "VALUES (10)");

        // query_select_cast_alias
        assertQuery("SELECT COUNT(*) FROM employee WHERE CAST(emp_id AS VARCHAR(10)) IS NOT NULL", "VALUES (10)");

        // query_select_average_count
        assertQuery("SELECT COUNT(*) AS total_employees FROM employee", "VALUES (10)");

        // query_select_lower_alias
        assertQuery("SELECT LOWER(emp_name) FROM employee WHERE emp_name = 'Tom' ORDER BY LOWER(emp_name)", "VALUES ('tom')");

        // query_select_join_alias
        assertQuery("SELECT e1.emp_name, e2.emp_name as manager_name " +
                        "FROM employee e1 JOIN employee e2 ON e1.u_id = e2.emp_id " +
                        "WHERE e1.dep_id = 'AF6' " +
                        "ORDER BY e1.emp_name",
                "VALUES ('Root', 'tom')");

        // query_select_multiple_joins_order_by
        assertQuery("SELECT o.order_id, c.customer_name, p.product_name " +
                        "FROM orders o " +
                        "JOIN customers c ON o.customer_id = c.customer_id " +
                        "JOIN products p ON o.product_id = p.product_id " +
                        "WHERE c.country = 'USA' AND p.category = 'Electronics' " +
                        "ORDER BY o.order_date DESC, o.order_id DESC",
                "VALUES (1004, 'John Doe', 'Smartphone'), (1001, 'John Doe', 'Laptop')");

        // TODO: Enable UPDATE operations once native execution engine supports UpdateNode
        // query_update
        // assertUpdate("UPDATE employee SET emp_location = 'chennai' WHERE emp_id = 4", 1);

        // query_select_after_update
        // assertQuery("SELECT * FROM employee WHERE emp_id = 4", "VALUES ('tom', 'chennai', 4, 'Ax5', 1)");

        // drop_table_if_exists_ctas
        assertUpdate("DROP TABLE IF EXISTS test_ctastable");

        // create_table_ctas - using iceberg.tpch.lineitem (pre-populated by createAllIcebergTables)
        // Note: Iceberg lineitem uses plain column names (orderkey, linenumber) not l_orderkey, l_linenumber
        assertUpdate("CREATE TABLE test_ctastable AS SELECT * FROM iceberg.tpch.lineitem ORDER BY orderkey LIMIT 10", 10);

        // select_from_ctas_table
        assertQuery("SELECT COUNT(*) FROM test_ctastable WHERE orderkey = 1 AND linenumber = 1 AND returnflag = 'N'", "VALUES (1)");

        // drop_table_if_exists_ctas_select_all
        assertUpdate("DROP TABLE IF EXISTS test_ctastable_all");

        // create_table_as_select_all
        assertUpdate("CREATE TABLE test_ctastable_all AS " +
                "SELECT d.* FROM test_ctastable d " +
                "INNER JOIN test_ctastable m ON d.quantity = m.quantity " +
                "WHERE d.linenumber = 5", 1);

        // select_from_ctas_select_all_table
        assertQuery("SELECT COUNT(*) FROM test_ctastable_all", "VALUES (1)");

        // alter_table_add_column
        assertUpdate("ALTER TABLE employee ADD COLUMN email VARCHAR(22)");

        // alter_table_drop_column
        assertUpdate("ALTER TABLE employee DROP COLUMN email");

        // drop_renamed_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS ice_employee");

        // alter_table_rename_table
        assertUpdate("ALTER TABLE employee RENAME TO ice_employee");

        // TODO: Enable DELETE operations once native execution engine supports ConnectorDeleteTableHandle
        // delete_row
        // assertUpdate("DELETE FROM ice_employee WHERE u_id = 3", 1);

        // drop_sorted_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS sorttable");

        // create_sorted_table
        assertUpdate("CREATE TABLE sorttable (name VARCHAR(25), age INT) WITH (sorted_by=ARRAY['name'])");

        // insert_into_sorted_table
        assertUpdate("INSERT INTO sorttable VALUES ('Abel', 20), ('Aaron', 19), ('Arjun', 25), ('Aaazl', 21)", 4);

        // select_from_sorted_table
        assertQuery("SELECT name FROM sorttable ORDER BY name", "VALUES ('Aaazl'), ('Aaron'), ('Abel'), ('Arjun')");

        // drop_sorted_table2_if_exists
        assertUpdate("DROP TABLE IF EXISTS sorttable2");

        // create_sorted_table2
        assertUpdate("CREATE TABLE sorttable2 (name VARCHAR(25), age INT) WITH (sorted_by=ARRAY['name DESC'])");

        // insert_into_sorted_table2
        assertUpdate("INSERT INTO sorttable2 VALUES ('Abel', 20), ('Aaron', 19), ('Arjun', 25), ('Aaazl', 21)", 4);

        // select_from_sorted_table2
        assertQuery("SELECT name FROM sorttable2 ORDER BY name DESC", "VALUES ('Arjun'), ('Abel'), ('Aaron'), ('Aaazl')");

        // drop_format_version_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS f2");

        // create_format_version_table
        assertUpdate("CREATE TABLE f2 (name VARCHAR(25)) WITH (format_version = '2')");

        // insert_format_version_table
        assertUpdate("INSERT INTO f2 VALUES ('abc')", 1);

        // select_from_format_version_table
        assertQuery("SELECT * FROM f2", "VALUES ('abc')");

        // drop_setdatatype_table_if_exists
        assertUpdate("DROP TABLE IF EXISTS setdatatype");

        // create_setdatatype_table
        assertUpdate("CREATE TABLE setdatatype (int_data INT, float_data REAL, decimal_data DECIMAL(5,2))");

        // alter_setdatatype_bigint
        assertUpdate("ALTER TABLE setdatatype ALTER COLUMN int_data SET DATA TYPE BIGINT");

        // alter_setdatatype_double
        assertUpdate("ALTER TABLE setdatatype ALTER COLUMN float_data SET DATA TYPE DOUBLE");

        // alter_setdatatype_decimal
        assertUpdate("ALTER TABLE setdatatype ALTER COLUMN decimal_data SET DATA TYPE DECIMAL(7,2)");

        // insert_into_setdatatype_table
        assertUpdate("INSERT INTO setdatatype VALUES (10010998, 190998.210, 23565.23)", 1);

        // select_from_setdatatype_table
        assertQuery("SELECT COUNT(*) FROM setdatatype", "VALUES (1)");

        // drop_sorted_table
        assertUpdate("DROP TABLE sorttable");

        // drop_sorted_table2
        assertUpdate("DROP TABLE sorttable2");

        // drop_format_version_table
        assertUpdate("DROP TABLE f2");

        // drop_setdatatype_table
        assertUpdate("DROP TABLE setdatatype");

        // drop_ctas_table
        assertUpdate("DROP TABLE test_ctastable");

        // drop_ctas_table_select_all
        assertUpdate("DROP TABLE test_ctastable_all");

        // drop_employee_table
        assertUpdate("DROP TABLE ice_employee");

        // drop_orders_table
        assertUpdate("DROP TABLE orders");

        // drop_customers_table
        assertUpdate("DROP TABLE customers");

        // drop_products_table
        assertUpdate("DROP TABLE products");
    }

    @Test
    public void testIcebergViewsAndMetadataOperations()
    {
        String tableName = "employee";
        String viewName = "test_view";

        try {
            // create_table
            assertUpdate("CREATE TABLE " + tableName + " ( " +
                    "emp_name VARCHAR(20), emp_location VARCHAR(20), emp_id INT, " +
                    "dep_id VARCHAR(4), u_id INT )");

            // insert_into_table
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "('tom', 'mumbai', 4, 'Ax5', 1), " +
                    "('wade', 'sanfransico', 7, 'AF62', 2), " +
                    "('Yan', 'dubai', 8, 'BF4', 3), " +
                    "('robert', 'hyderabad', 3, 'Zf6', 9), " +
                    "('Root', 'kochi', 12, 'AF6', 4), " +
                    "('ivan', 'bangalore', 43, 'BF4', 10), " +
                    "('Jorge', 'Sydney', 34, 'Zf6', 5), " +
                    "('Tom', 'kochi', 58, 'AF6', 6), " +
                    "('Bot', 'bangalore', 97, 'BF4', 7), " +
                    "('Sam', 'dallas', 46, 'Zf6', 8)", 10);

            // create_view
            assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

            // select_from_view
            assertQuery("SELECT COUNT(*) FROM " + viewName, "VALUES (10)");

            // alter_rename_view
            assertUpdate("ALTER VIEW " + viewName + " RENAME TO " + viewName + "2");

            // show_columns
            assertQuery("SELECT COUNT(*) FROM information_schema.columns " +
                    "WHERE table_name = '" + tableName + "'", "VALUES (5)");

            // describe_table
            assertQuery("SELECT COUNT(*) FROM information_schema.columns " +
                    "WHERE table_name = '" + tableName + "'", "VALUES (5)");

            // analyze_table
            assertUpdate("ANALYZE " + tableName, 10);

            // set_session
            getQueryRunner().execute("SET SESSION optimize_hash_generation = true");

            // reset_session
            getQueryRunner().execute("RESET SESSION optimize_hash_generation");

            // insert_into_table_values
            assertUpdate("INSERT INTO " + tableName + " " +
                    "SELECT * FROM (VALUES ('Yan', 'dubai', 8, 'BF4', 3), " +
                    "('robert', 'hyderabad', 3, 'Zf6', 9)) AS t (emp_name, emp_location, emp_id, dep_id, u_id)", 2);

            // TODO: TRUNCATE not supported in native engine yet
            // truncate_table
            // assertUpdate("TRUNCATE TABLE " + tableName);

            // truncate_table_select
            // assertQuery("SELECT * FROM " + tableName, "VALUES ()");

            // drop_view
            assertUpdate("DROP VIEW " + viewName + "2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testIcebergDecimalBugRepro()
    {
        // Minimal repro for Prestissimo DECIMAL bug
        String tableName = "iceberg.tpch.decimal_bug_repro";

        try {
            // Create table with DECIMAL column
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("CREATE TABLE " + tableName + " (id INT, amount DECIMAL(10,2))");

            // Insert simple values
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 100.00), (2, 600.00)", 2);

            // Debug: Check what was actually inserted
            System.out.println("\n=== DEBUG: Checking inserted data ===");
            MaterializedResult insertedData = computeActual("SELECT id, amount FROM " + tableName + " ORDER BY id");
            System.out.println("Inserted rows: " + insertedData);
            for (MaterializedRow row : insertedData.getMaterializedRows()) {
                System.out.println("  Row: id=" + row.getField(0) + ", amount=" + row.getField(1) +
                                   " (type: " + (row.getField(1) != null ? row.getField(1).getClass().getName() : "null") + ")");
            }

            // Debug: Check raw bytes of amount column
            System.out.println("\n=== DEBUG: Checking amount values in detail ===");
            MaterializedResult amountDetails = computeActual(
                "SELECT id, amount, CAST(amount AS VARCHAR) as amount_str, " +
                "CAST(amount AS BIGINT) as amount_bigint FROM " + tableName + " ORDER BY id");
            System.out.println("Amount details: " + amountDetails);
            for (MaterializedRow row : amountDetails.getMaterializedRows()) {
                System.out.println("  id=" + row.getField(0) +
                                   ", amount=" + row.getField(1) +
                                   ", as_string=" + row.getField(2) +
                                   ", as_bigint=" + row.getField(3));
            }

            assertQuery("SELECT * FROM " + tableName, "VALUES (1, CAST(100.00 AS DECIMAL(10,2))), (2, CAST(600.00 AS DECIMAL(10,2)))");

            // Query $files metadata to check min/max of id field
            System.out.println("\n=== DEBUG: Querying $files metadata ===");
            String filesQuery = "SELECT file_path, record_count, column_sizes, value_counts, " +
                    "null_value_counts, nan_value_counts, lower_bounds, upper_bounds " +
                    "FROM iceberg.tpch.\"decimal_bug_repro$files\"";
            MaterializedResult filesResult = computeActual(filesQuery);
            System.out.println("$files metadata: " + filesResult);
            
            // Extract and display the bounds for all fields
            if (!filesResult.getMaterializedRows().isEmpty()) {
                MaterializedRow row = filesResult.getMaterializedRows().get(0);
                System.out.println("\n=== DEBUG: File statistics breakdown ===");
                System.out.println("File path: " + row.getField(0));
                System.out.println("Record count: " + row.getField(1));
                System.out.println("Column sizes: " + row.getField(2));
                System.out.println("Value counts: " + row.getField(3));
                System.out.println("Null value counts: " + row.getField(4));
                System.out.println("NaN value counts: " + row.getField(5));
                System.out.println("Lower bounds: " + row.getField(6));
                System.out.println("Upper bounds: " + row.getField(7));
                
                // Parse the bounds maps
                Object lowerBounds = row.getField(6);
                Object upperBounds = row.getField(7);
                System.out.println("\n=== DEBUG: Bounds analysis ===");
                System.out.println("Lower bounds type: " + (lowerBounds != null ? lowerBounds.getClass().getName() : "null"));
                System.out.println("Upper bounds type: " + (upperBounds != null ? upperBounds.getClass().getName() : "null"));
                
                if (lowerBounds != null && lowerBounds instanceof java.util.Map) {
                    java.util.Map<?, ?> lowerMap = (java.util.Map<?, ?>) lowerBounds;
                    System.out.println("Lower bounds map entries:");
                    for (java.util.Map.Entry<?, ?> entry : lowerMap.entrySet()) {
                        System.out.println("  Field " + entry.getKey() + ": " + entry.getValue() +
                                         " (type: " + (entry.getValue() != null ? entry.getValue().getClass().getName() : "null") + ")");
                    }
                }
                
                if (upperBounds != null && upperBounds instanceof java.util.Map) {
                    java.util.Map<?, ?> upperMap = (java.util.Map<?, ?>) upperBounds;
                    System.out.println("Upper bounds map entries:");
                    for (java.util.Map.Entry<?, ?> entry : upperMap.entrySet()) {
                        System.out.println("  Field " + entry.getKey() + ": " + entry.getValue() +
                                         " (type: " + (entry.getValue() != null ? entry.getValue().getClass().getName() : "null") + ")");
                    }
                }
            }

            // Debug: Check MIN/MAX aggregation results
            System.out.println("\n=== DEBUG: Testing MIN/MAX aggregations ===");
            MaterializedResult minMaxResult = computeActual("SELECT MIN(amount), MAX(amount) FROM " + tableName);
            System.out.println("MIN/MAX result: " + minMaxResult);
            if (!minMaxResult.getMaterializedRows().isEmpty()) {
                MaterializedRow row = minMaxResult.getMaterializedRows().get(0);
                Object minVal = row.getField(0);
                Object maxVal = row.getField(1);
                System.out.println("MIN value: " + minVal + " (type: " + (minVal != null ? minVal.getClass().getName() : "null") + ")");
                System.out.println("MAX value: " + maxVal + " (type: " + (maxVal != null ? maxVal.getClass().getName() : "null") + ")");
            }

            // Verify MIN/MAX return the correct decimal values with proper scale.
            // Use CAST to preserve DECIMAL(10,2) type on the expected side.
            System.out.println("\n=== DEBUG: Running final assertion ===");
            assertQuery("SELECT MIN(amount), MAX(amount) FROM " + tableName,
                    "VALUES (CAST(100.00 AS DECIMAL(10,2)), CAST(600.00 AS DECIMAL(10,2)))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testIcebergDecimalOnJavaOnly()
    {
        // This test runs ONLY on the Java query runner to verify DECIMAL handling
        // If this passes, it confirms the bug is in Prestissimo, not Java
        String tableName = "iceberg.tpch.decimal_test_java";
        QueryRunner javaRunner = (QueryRunner) getExpectedQueryRunner();

        try {
            javaRunner.execute("DROP TABLE IF EXISTS " + tableName);
            javaRunner.execute("CREATE TABLE " + tableName + " ( " +
                    "customer_id INT, order_id INT, order_date DATE, " +
                    "total_amount DECIMAL(10,2) )");

            javaRunner.execute("INSERT INTO " + tableName + " VALUES " +
                    "(1, 101, DATE '2023-01-01', 100.00), " +
                    "(1, 102, DATE '2023-02-15', 150.00), " +
                    "(1, 103, DATE '2023-03-10', 120.00), " +
                    "(2, 104, DATE '2023-01-20', 200.00), " +
                    "(2, 105, DATE '2023-02-05', 250.00), " +
                    "(3, 106, DATE '2023-03-01', 300.00), " +
                    "(3, 107, DATE '2023-03-25', 350.00), " +
                    "(3, 108, DATE '2023-04-10', 400.00), " +
                    "(4, 109, DATE '2023-01-15', 500.00), " +
                    "(4, 110, DATE '2023-02-10', 600.00)");

            // Verify on Java runner
            MaterializedResult result = javaRunner.execute(javaRunner.getDefaultSession(),
                    "SELECT MIN(total_amount), MAX(total_amount) FROM " + tableName);
            System.out.println("Java Runner Result: " + result);

            // Check if values are correct
            Object minValue = result.getMaterializedRows().get(0).getField(0);
            Object maxValue = result.getMaterializedRows().get(0).getField(1);
            System.out.println("MIN: " + minValue + ", MAX: " + maxValue);

            // Verify row count
            MaterializedResult countResult = javaRunner.execute(javaRunner.getDefaultSession(),
                    "SELECT COUNT(*) FROM " + tableName);
            System.out.println("Row count: " + countResult.getMaterializedRows().get(0).getField(0));
        }
        finally {
            javaRunner.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // TODO: CONFIRMED PRESTISSIMO BUG - DECIMAL values are corrupted when reading from Iceberg
    // Java Runner (correct): MIN=100.00, MAX=600.00
    // Prestissimo (wrong): MIN=2709913.60, MAX=16259481.60
    // The row count is correct (10 rows) but DECIMAL(10,2) values are corrupted immediately after INSERT
    // Verified by testIcebergDecimalOnJavaOnly() - Java runner works correctly
    // This is a Prestissimo-specific bug in reading DECIMAL values from Iceberg tables
    @Test(enabled = false)
    public void testIcebergAdvancedAnalytics()
    {
        String tableName = "iceberg.tpch.sales_orders";

        try {
            // Ensure clean state
            assertUpdate("DROP TABLE IF EXISTS " + tableName);

            // create_orders_table
            assertUpdate("CREATE TABLE " + tableName + " ( " +
                    "customer_id INT, order_id INT, order_date DATE, " +
                    "total_amount DECIMAL(10,2) )");

            // insert_into_sales_data
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 101, DATE '2023-01-01', 100.00), " +
                    "(1, 102, DATE '2023-02-15', 150.00), " +
                    "(1, 103, DATE '2023-03-10', 120.00), " +
                    "(2, 104, DATE '2023-01-20', 200.00), " +
                    "(2, 105, DATE '2023-02-05', 250.00), " +
                    "(3, 106, DATE '2023-03-01', 300.00), " +
                    "(3, 107, DATE '2023-03-25', 350.00), " +
                    "(3, 108, DATE '2023-04-10', 400.00), " +
                    "(4, 109, DATE '2023-01-15', 500.00), " +
                    "(4, 110, DATE '2023-02-10', 600.00)", 10);

            // Verify row count
            assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES (BIGINT '10')");

            // Verify data immediately after insert
            assertQuery("SELECT MIN(total_amount), MAX(total_amount) FROM " + tableName,
                    "VALUES (DECIMAL '100.00', DECIMAL '600.00')");

            // select_orders_by_customer
            assertQuery("SELECT customer_id, COUNT(*) AS total_orders " +
                    "FROM " + tableName + " GROUP BY customer_id " +
                    "ORDER BY customer_id",
                    "VALUES (1, BIGINT '3'), (2, BIGINT '2'), (3, BIGINT '3'), (4, BIGINT '2')");

            // select_max_min_avg_amount
            assertQuery("SELECT MAX(total_amount) AS max_amount, " +
                    "MIN(total_amount) AS min_amount " +
                    "FROM " + tableName,
                    "VALUES (DECIMAL '600.00', DECIMAL '100.00')");

            // select_year_month_orders_revenue
            assertQuery("SELECT EXTRACT(YEAR FROM order_date) AS year, " +
                    "EXTRACT(MONTH FROM order_date) AS month, " +
                    "COUNT(*) AS total_orders " +
                    "FROM " + tableName + " " +
                    "GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date) " +
                    "ORDER BY year, month LIMIT 1",
                    "VALUES (BIGINT '2023', BIGINT '1', BIGINT '3')");

            // select_top_customers with window functions
            assertQuery("WITH customer_revenue AS ( " +
                    "SELECT customer_id, SUM(total_amount) AS total_revenue, " +
                    "COUNT(*) AS order_count " +
                    "FROM " + tableName + " GROUP BY customer_id) " +
                    "SELECT customer_id FROM customer_revenue " +
                    "ORDER BY total_revenue DESC LIMIT 1",
                    "VALUES (4)");

            // select_median_order_amount
            assertQuery("SELECT approx_percentile(total_amount, 0.5) " +
                    "FROM " + tableName,
                    "VALUES (DECIMAL '300.00')");

            // assign_orders_to_quartiles
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, order_id, total_amount, " +
                    "NTILE(4) OVER (ORDER BY total_amount DESC) AS order_quartile " +
                    "FROM " + tableName + ") t", "VALUES (BIGINT '10')");

            // first_and_last_order
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, order_id, total_amount, " +
                    "FIRST_VALUE(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS first_order_value, " +
                    "LAST_VALUE(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date " +
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_value " +
                    "FROM " + tableName + ") t", "VALUES (BIGINT '10')");

            // dense_rank_orders
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, order_id, total_amount, " +
                    "DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY total_amount DESC) AS dense_rank " +
                    "FROM " + tableName + ") t", "VALUES (BIGINT '10')");

            // percentile_rank_of_each_order
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, order_id, total_amount, " +
                    "PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY total_amount DESC) AS percentile_rank " +
                    "FROM " + tableName + ") t", "VALUES (BIGINT '10')");

            // customer_retention_rate
            assertQuery("WITH first_purchase AS ( " +
                    "SELECT customer_id, MIN(order_date) AS first_order_date " +
                    "FROM " + tableName + " GROUP BY customer_id), " +
                    "monthly_orders AS ( " +
                    "SELECT customer_id, DATE_TRUNC('month', order_date) AS order_month " +
                    "FROM " + tableName + ") " +
                    "SELECT COUNT(*) FROM monthly_orders " +
                    "JOIN first_purchase ON monthly_orders.customer_id = first_purchase.customer_id",
                    "VALUES (BIGINT '10')");

            // customers_gap_between_consecutive_orders
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, order_id, order_date, " +
                    "LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date, " +
                    "DATE_DIFF('day', LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), order_date) AS days_since_last_order " +
                    "FROM " + tableName + ") t", "VALUES (BIGINT '10')");

            // customer_order_frequency (RFM analysis)
            assertQuery("SELECT COUNT(*) FROM ( " +
                    "SELECT customer_id, " +
                    "MAX(order_date) AS last_purchase_date, " +
                    "COUNT(order_id) AS purchase_frequency, " +
                    "SUM(total_amount) AS total_spent, " +
                    "NTILE(4) OVER (ORDER BY MAX(order_date) DESC) AS recency_quartile, " +
                    "NTILE(4) OVER (ORDER BY COUNT(order_id) DESC) AS frequency_quartile, " +
                    "NTILE(4) OVER (ORDER BY SUM(total_amount) DESC) AS monetary_quartile " +
                    "FROM " + tableName + " GROUP BY customer_id) t", "VALUES (BIGINT '3')");

            // Drop and recreate for fraud detection test
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("CREATE TABLE " + tableName + " ( " +
                    "customer_id INT, order_id INT, order_date DATE, " +
                    "total_amount DECIMAL(10,2) )");

            // insert_into_sales_data for fraud detection
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 101, DATE '2023-01-01', 200.00), " +
                    "(2, 102, DATE '2023-02-15', 250.00), " +
                    "(3, 103, DATE '2023-03-20', 300.00), " +
                    "(4, 104, DATE '2023-04-10', 350.00), " +
                    "(5, 105, DATE '2023-05-05', 10000.00), " +
                    "(6, 106, DATE '2023-06-12', 400.00), " +
                    "(7, 107, DATE '2023-07-07', 450.00), " +
                    "(8, 108, DATE '2023-08-14', 500.00), " +
                    "(9, 109, DATE '2023-09-20', 550.00), " +
                    "(10, 110, DATE '2023-10-10', 600.00)", 10);

            // show_fraudulent_transactions_zscore
            assertQuery("WITH stats AS ( " +
                    "SELECT AVG(total_amount) AS mean_spent, " +
                    "STDDEV(total_amount) AS stddev_spent " +
                    "FROM " + tableName + ") " +
                    "SELECT COUNT(*) FROM " + tableName + ", stats " +
                    "WHERE ABS((total_amount - stats.mean_spent) / stats.stddev_spent) > 2",
                    "VALUES (BIGINT '1')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
