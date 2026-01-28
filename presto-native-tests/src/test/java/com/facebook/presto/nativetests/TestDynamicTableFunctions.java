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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static java.lang.Boolean.parseBoolean;

/**
 * Test suite for dynamically loaded table functions using the TestingTableFunctionsPlugin.
 * This test validates that table functions can be loaded from a shared library (.so/.dylib)
 * at runtime and executed correctly.
 * 
 * The plugin provides three test table functions:
 * - repeat_table_function: Repeats input rows a specified number of times
 * - identity_table_function: Returns input rows unchanged
 * - simple_table_function: Returns a simple boolean column
 */
public class TestDynamicTableFunctions
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.getLogger(TestDynamicTableFunctions.class);
    private String storageFormat;

    @BeforeSuite
    public void buildNativeLibrary()
            throws IOException, InterruptedException
    {
        Path localPluginDir = getLocalPluginDirectory();
        
        // Check if the testing plugin library already exists in local directory
        if (Files.exists(localPluginDir) && hasPluginLibrary(localPluginDir)) {
            logger.info("Testing table functions plugin library found in local directory, skipping build");
            return;
        }
        
        logger.info("Testing table functions plugin library not found locally, building...");
        Path prestoRoot = findPrestoRoot();
        
        // Build the plugin using the local Makefile in presto-native-tests
        String workingDir = prestoRoot
                .resolve("presto-native-tests/presto_cpp/tests/custom_tvf_functions").toAbsolutePath().toString();
        
        ProcessBuilder builder = new ProcessBuilder("make", "release");
        builder.directory(new File(workingDir));
        builder.redirectErrorStream(true);
        
        Process process = builder.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[BUILD OUTPUT] " + line);
            }
        }
        
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("C++ build failed with exit code " + exitCode);
        }
        logger.info("Testing table functions plugin library built successfully at: " + localPluginDir);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        Path pluginDir = getLocalPluginDirectory();
        if (!Files.exists(pluginDir) || !hasPluginLibrary(pluginDir)) {
            throw new IllegalStateException(
                    "Plugin library not found in: " + pluginDir +
                    ". Please ensure the build completed successfully.");
        }
        
        return nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setPluginDirectory(Optional.of(pluginDir.toString()))
                .build();
    }

    @Test
    public void testRepeatTableFunction()
    {
        // Test repeating a simple table
        assertQuery(
                "SELECT * FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT 1 as x, 'a' as y), " +
                        "    COUNT => 3))",
                "VALUES (1, 'a'), (1, 'a'), (1, 'a')");

        // Test with multiple input rows
        assertQuery(
                "SELECT * FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(x, y)), " +
                        "    COUNT => 2))",
                "VALUES (1, 'a'), (1, 'a'), (2, 'b'), (2, 'b')");

        // Test with COUNT = 1 (should return original rows)
        assertQuery(
                "SELECT * FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT 42 as num), " +
                        "    COUNT => 1))",
                "VALUES 42");

        // Test with larger COUNT
        assertQuery(
                "SELECT count(*) FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2, 3) t(x)), " +
                        "    COUNT => 5))",
                "VALUES BIGINT '15'");
    }

    @Test
    public void testIdentityTableFunction()
    {
        // Test identity with single column
        assertQuery(
                "SELECT * FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(SELECT 1 as x)))",
                "VALUES 1");

        // Test identity with multiple columns
        assertQuery(
                "SELECT * FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(SELECT 1 as x, 'hello' as y, true as z)))",
                "VALUES (1, 'hello', true)");

        // Test identity with multiple rows
        assertQuery(
                "SELECT * FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(num, letter))))",
                "VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        // Test identity preserves order
        assertQuery(
                "SELECT * FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 5, 3, 1, 4, 2) t(x) ORDER BY x)))",
                "VALUES 1, 2, 3, 4, 5");
    }

    @Test
    public void testSimpleTableFunction()
    {
        // Test simple table function with column name
        assertQuery(
                "SELECT * FROM TABLE(simple_table_function(COLUMN => 'test_col'))",
                "SELECT test_col FROM (VALUES true) t(test_col)");

        // Test with different column name
        assertQuery(
                "SELECT * FROM TABLE(simple_table_function(COLUMN => 'my_boolean'))",
                "SELECT my_boolean FROM (VALUES true) t(my_boolean)");
    }

    @Test
    public void testTableFunctionComposition()
    {
        // Test composing repeat and identity functions
        assertQuery(
                "SELECT * FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(repeat_table_function(" +
                        "        INPUT => TABLE(SELECT 1 as x), " +
                        "        COUNT => 3))))",
                "VALUES 1, 1, 1");

        // Test more complex composition
        assertQuery(
                "SELECT count(*) FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(identity_table_function(" +
                        "        INPUT => TABLE(SELECT * FROM (VALUES 1, 2) t(x)))), " +
                        "    COUNT => 4))",
                "VALUES BIGINT '8'");
    }

    @Test
    public void testTableFunctionWithAggregation()
    {
        // Test aggregation over repeated rows
        assertQuery(
                "SELECT sum(x), count(*) FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2, 3) t(x)), " +
                        "    COUNT => 2))",
                "VALUES (BIGINT '12', BIGINT '6')");

        // Test distinct after repeat
        assertQuery(
                "SELECT DISTINCT x FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2, 3) t(x)), " +
                        "    COUNT => 5))",
                "VALUES 1, 2, 3");
    }

    @Test
    public void testTableFunctionWithJoin()
    {
        // Test joining table function output with regular table
        assertQuery(
                "SELECT t1.x, t2.y " +
                        "FROM TABLE(identity_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2) t(x)))) t1 " +
                        "JOIN (VALUES ('a'), ('b')) t2(y) ON true",
                "VALUES (1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')");
    }

    @Test
    public void testTableFunctionWithFilter()
    {
        // Test filtering table function output
        assertQuery(
                "SELECT * FROM TABLE(repeat_table_function(" +
                        "    INPUT => TABLE(SELECT * FROM (VALUES 1, 2, 3, 4, 5) t(x)), " +
                        "    COUNT => 2)) " +
                        "WHERE x > 2",
                "VALUES 3, 3, 4, 4, 5, 5");
    }

    /**
     * Get the local plugin directory within presto-native-tests module.
     * This is where the plugin libraries are built.
     */
    private static Path getLocalPluginDirectory()
    {
        Path prestoRoot = findPrestoRoot();
        
        // Check both debug and release build directories
        List<Path> candidates = ImmutableList.of(
                prestoRoot.resolve("presto-native-tests/presto_cpp/tests/custom_tvf_functions/_build/release/custom_tvf_functions"),
                prestoRoot.resolve("presto-native-tests/presto_cpp/tests/custom_tvf_functions/_build/debug/custom_tvf_functions"));
        
        // Return the first one that exists, or the release path as default
        return candidates.stream()
                .filter(Files::exists)
                .findFirst()
                .orElse(candidates.get(0));
    }

    /**
     * Check if the plugin directory contains the required shared library.
     */
    private static boolean hasPluginLibrary(Path pluginDir)
    {
        if (!Files.exists(pluginDir)) {
            return false;
        }
        
        try (Stream<Path> files = Files.list(pluginDir)) {
            return files.anyMatch(path -> {
                String name = path.getFileName().toString();
                return name.startsWith("libpresto_testing_tvf_plugin") &&
                       (name.endsWith(".so") || name.endsWith(".dylib"));
            });
        }
        catch (IOException e) {
            return false;
        }
    }


    private static Path findPrestoRoot()
    {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("presto-native-tests")) ||
                    Files.exists(dir.resolve("presto-native-execution"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Could not locate presto root directory.");
    }
}
