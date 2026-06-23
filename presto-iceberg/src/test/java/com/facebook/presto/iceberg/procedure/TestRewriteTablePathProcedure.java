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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ManifestFile;
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
import java.util.List;
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
    public void testRewriteTablePathEndToEnd()
    {
        // Full end-to-end: create table, rewrite paths, copy data files, register, query.
        String sourceName = "rewrite_e2e_source";
        String targetName = "rewrite_e2e_target";
        createTable(sourceName);
        try {
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + sourceName + " VALUES (2, 'b')", 1);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_e2e_target";
            String targetLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            // Step 1: Rewrite all metadata files to the target location.
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix));

            // Step 2: Copy data files from source to target (same directory structure).
            copyDirectory(
                    java.nio.file.Paths.get(originalLocation.replace("file:", ""), "data"),
                    java.nio.file.Paths.get(targetLocation.replace("file:", ""), "data"));

            // Step 3: Register the rewritten table under a new name using the new metadata.
            String targetMetadataDir = targetLocation + "/metadata";
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Step 4: Verify the registered table returns the same data.
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a'), (2, 'b')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    /**
     * Recursively copies all files from {@code source} to {@code target}, creating
     * intermediate directories as needed. Skips if the source directory does not exist
     * (table may have no data files if never inserted into).
     */
    private static void copyDirectory(java.nio.file.Path source, java.nio.file.Path target)
    {
        if (!source.toFile().exists()) {
            return;
        }
        try {
            Files.walk(source).forEach(sourcePath -> {
                java.nio.file.Path targetPath = target.resolve(source.relativize(sourcePath));
                try {
                    if (java.nio.file.Files.isDirectory(sourcePath)) {
                        java.nio.file.Files.createDirectories(targetPath);
                    }
                    else {
                        java.nio.file.Files.createDirectories(targetPath.getParent());
                        java.nio.file.Files.copy(sourcePath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to copy " + sourcePath + " -> " + targetPath, e);
                }
            });
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to walk source directory: " + source, e);
        }
    }

    @Test
    public void testRewriteTablePathManifestListRewritten()
    {
        String tableName = "rewrite_table_path_manifest_list";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_ml_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            // Capture the manifest list path before calling the procedure.
            String manifestListLocation = table.currentSnapshot().manifestListLocation();

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // The target manifest list Avro file must physically exist at the rewritten path.
            String targetManifestListPath = manifestListLocation.replace(sourcePrefix, targetPrefix);
            assertTrue(new File(targetManifestListPath.replace("file:", "")).exists(),
                    "Manifest list file should exist at " + targetManifestListPath);

            // The new metadata JSON must point to the rewritten manifest list path.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            newMetadata.snapshots().forEach(snapshot -> {
                if (snapshot.manifestListLocation() != null) {
                    assertTrue(snapshot.manifestListLocation().startsWith(targetPrefix),
                            format("Snapshot manifest list '%s' should start with '%s'",
                                    snapshot.manifestListLocation(), targetPrefix));
                }
            });
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathManifestFileRewritten()
    {
        String tableName = "rewrite_table_path_manifest_file";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_mf_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            // Capture the manifest file paths before calling the procedure.
            List<String> manifestPaths = table.currentSnapshot()
                    .allManifests(((BaseTable) table).operations().io())
                    .stream()
                    .map(ManifestFile::path)
                    .collect(java.util.stream.Collectors.toList());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // Each manifest Avro file must physically exist at the target path.
            for (String manifestPath : manifestPaths) {
                String targetManifestPath = manifestPath.replace(sourcePrefix, targetPrefix);
                assertTrue(new File(targetManifestPath.replace("file:", "")).exists(),
                        "Manifest file should exist at " + targetManifestPath);
            }

            // The new metadata must also be readable.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(newMetadata.location(), expectedNewLocation);
        }
        finally {
            dropTable(tableName);
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
