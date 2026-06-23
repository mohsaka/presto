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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRewriteTablePathProcedure
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().setCatalogType(HADOOP).build().getQueryRunner();
    }

    private void createTable(String tableName)
    {
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value VARCHAR)");
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    private Table loadTable(String tableName)
    {
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        return ImmutableMap.of("warehouse", getCatalogDirectory().toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false).toFile();
    }

    @Test
    public void testRewriteTablePathPositionalArgs()
    {
        String tableName = "rewrite_table_path_positional";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // The catalog entry must be unchanged — the procedure does not update it.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Find the latest metadata file written to the target directory and verify its location.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(newMetadata.location(), expectedNewLocation,
                    format("New metadata location should be '%s' but was '%s'", expectedNewLocation, newMetadata.location()));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathNamedArgs()
    {
        String tableName = "rewrite_table_path_named";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_renamed";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path(schema => '%s', table_name => '%s', source_prefix => '%s', target_prefix => '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // Catalog entry unchanged.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Find the latest metadata file written to the target directory and verify its location.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertTrue(newMetadata.location().startsWith(targetPrefix),
                    format("New metadata location should start with '%s' but was '%s'", targetPrefix, newMetadata.location()));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathSnapshotPointersRewritten()
    {
        String tableName = "rewrite_table_path_snapshots";
        createTable(tableName);
        try {
            // Two inserts produce two snapshots, each with their own manifest-list pointer.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_snap_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // Verify all snapshot manifest-list pointers in the new metadata use the target prefix.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            newMetadata.snapshots().forEach(snapshot -> {
                if (snapshot.manifestListLocation() != null) {
                    assertTrue(snapshot.manifestListLocation().startsWith(targetPrefix),
                            format("Snapshot %s manifest list '%s' should start with '%s'",
                                    snapshot.snapshotId(), snapshot.manifestListLocation(), targetPrefix));
                }
            });
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Resolves the latest {@code .metadata.json} file written under
     * {@code <tableLocation>/metadata/} and parses it.
     * This avoids hardcoding the metadata version number (v1, v2, v3, …) which
     * advances whenever the table receives new snapshots.
     */
    private TableMetadata readLatestTargetMetadata(String tableLocation)
    {
        // tableLocation is a file: URI — strip the scheme to get a local path.
        String localPath = tableLocation.startsWith("file:") ? tableLocation.substring("file:".length()) : tableLocation;
        java.nio.file.Path metadataDir = java.nio.file.Paths.get(localPath, "metadata");

        try {
            Optional<Path> latest = Files.list(metadataDir)
                    .filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()));
            assertTrue(latest.isPresent(), "No metadata file found under " + metadataDir);
            return TableMetadataParser.read(null,
                    HadoopInputFile.fromLocation("file:" + latest.get().toAbsolutePath(), new Configuration()));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to list metadata directory: " + metadataDir, e);
        }
    }

    @Test
    public void testRewriteTablePathWithNonMatchingPrefixFails()
    {
        String tableName = "rewrite_table_path_bad_prefix";
        createTable(tableName);
        try {
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', 'file://wrong/warehouse', 'file://new/warehouse')",
                            TEST_SCHEMA, tableName),
                    "Table location .* does not start with source prefix 'file://wrong/warehouse'");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidRewriteTablePathCalls()
    {
        assertQueryFails("CALL system.rewrite_table_path('schema', 'table', 'src')",
                "line 1:1: Required procedure argument 'target_prefix' is missing");
        assertQueryFails("CALL custom.rewrite_table_path('tpch', 'test', 'src', 'dst')",
                "Procedure not registered: custom.rewrite_table_path");
        assertQueryFails("CALL system.rewrite_table_path(table_name => 'test', source_prefix => 'src', target_prefix => 'dst')",
                "line 1:1: Required procedure argument 'schema' is missing");
    }

    @Test
    public void testRewriteTablePathOnNonExistingTableFails()
    {
        assertQueryFails("CALL system.rewrite_table_path('tpch', 'non_existing_table', 'src', 'dst')",
                "Table does not exist: tpch.non_existing_table");
    }
}
