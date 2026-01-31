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
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

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

        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setPluginDirectory(Optional.of(pluginDir.toString()))
                .setCoordinatorSidecarEnabled(true)
                .setLoadTvfPlugin(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
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
                "VALUES 15");
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
/*
    // Tests from TestTableFunctionInvocation
    @Test
    public void testPrimitiveDefaultArgument()
    {
        assertQuery("SELECT boolean_column FROM TABLE(simple_table_function(column => 'boolean_column', ignored => 1))", "SELECT true WHERE false");

        // skip the `ignored` argument.
        assertQuery("SELECT boolean_column FROM TABLE(simple_table_function(column => 'boolean_column'))",
                "SELECT true WHERE false");
    }

    @Test
    public void testNoArgumentsPassed()
    {
        assertQuery("SELECT col FROM TABLE(simple_table_function())",
                "SELECT true WHERE false");
    }*/

    @Test
    public void testIdentityFunction()
    {
        assertQuery("SELECT b, a FROM TABLE(identity_table_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))",
                "VALUES (2, 1), (4, 3), (6, 5)");

        // null partitioning value
        assertQuery("SELECT i.b, a FROM TABLE(identity_table_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b)) i",
                "VALUES (1, 'x'), (2, 'y'), (null, 'z')");
    }

    @Test
    public void testRepeatFunction()
    {
        assertQuery("SELECT * FROM TABLE(repeat_table_function(TABLE(VALUES (1, 2), (3, 4), (5, 6))))",
                "VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertQuery("SELECT * FROM TABLE(repeat_table_function(TABLE(VALUES ('a', true), ('b', false)), 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(repeat_table_function(TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x, 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(repeat_table_function(TABLE(VALUES ('a', true), ('b', false)) t(x, y) ORDER BY y, 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertQuery("SELECT * FROM TABLE(repeat_table_function(TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x ORDER BY y, 4))",
                "VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");
    }

    private static Path getLocalPluginDirectory()
    {
        Path prestoRoot = findPrestoRoot();
        // Check both debug and release build directories
        List<Path> candidates = ImmutableList.of(
                prestoRoot.resolve("presto-native-tests/_build/debug/presto_cpp/tests/custom_tvf_functions"),
                prestoRoot.resolve("presto-native-tests/_build/release/presto_cpp/tests/custom_tvf_functions"));
        // Return the first one that exists, or the release path as default
        return candidates.stream()
                .filter(Files::exists)
                .findFirst()
                .orElse(candidates.get(0));
    }

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
