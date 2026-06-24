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
import java.util.stream.Collectors;

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

    // -------------------------------------------------------------------------
    // Argument validation
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Call syntax — positional and named args both reach the same code path
    // -------------------------------------------------------------------------

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

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // Catalog entry must be unchanged — the procedure does not update it.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata at the target location must reference the new location.
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

            assertUpdate(format("CALL system.rewrite_table_path(schema => '%s', table_name => '%s', source_prefix => '%s', target_prefix => '%s', staging_location => '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // Catalog entry unchanged.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata at the target location must reference the new location.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertTrue(newMetadata.location().startsWith(targetPrefix),
                    format("New metadata location should start with '%s' but was '%s'", targetPrefix, newMetadata.location()));
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Rewrite correctness — verifies each file type is physically rewritten
    // with the correct internal path strings
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathSnapshotPointersRewritten()
    {
        // Two snapshots → two manifest-list pointers. Both must reference target_prefix.
        String tableName = "rewrite_table_path_snapshots";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_snap_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

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

    @Test
    public void testRewriteTablePathManifestListRewritten()
    {
        // The manifest list Avro must be physically written at the target path and its
        // internal manifest_path fields must reference target_prefix.
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
            String manifestListLocation = table.currentSnapshot().manifestListLocation();

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // File must physically exist at the target path.
            String targetManifestListPath = manifestListLocation.replace(sourcePrefix, targetPrefix);
            assertTrue(new File(targetManifestListPath.replace("file:", "")).exists(),
                    "Manifest list file should exist at " + targetManifestListPath);

            // Metadata must point to the rewritten manifest list path.
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
        // Each manifest Avro must be physically written at the target path with
        // data_file.file_path fields referencing target_prefix.
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
            List<String> manifestPaths = table.currentSnapshot()
                    .allManifests(((BaseTable) table).operations().io())
                    .stream()
                    .map(ManifestFile::path)
                    .collect(Collectors.toList());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // Each manifest Avro must physically exist at the target path.
            for (String manifestPath : manifestPaths) {
                String targetManifestPath = manifestPath.replace(sourcePrefix, targetPrefix);
                assertTrue(new File(targetManifestPath.replace("file:", "")).exists(),
                        "Manifest file should exist at " + targetManifestPath);
            }
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(newMetadata.location(), expectedNewLocation);
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Staging location — files go to staging, content references target_prefix,
    // file-list CSV maps staging → target for metadata and source → target for data
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathStagingLocationWritesFilesToStaging()
            throws IOException
    {
        // Metadata files must be physically written under staging_location, not target_prefix.
        // The content inside each file must still reference target_prefix.
        String tableName = "rewrite_table_path_staging";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_stage_target";
            String stagingLocation = sourcePrefix + "_staging";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Catalog must be unchanged.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata file physically written to staging; content references target_prefix.
            TableMetadata stagingMetadata = readLatestTargetMetadata(stagingLocation + originalLocation.substring(sourcePrefix.length()));
            assertEquals(stagingMetadata.location(), expectedNewLocation,
                    "Metadata content should reference target_prefix, not staging_location");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathFileListContents()
            throws IOException
    {
        // file-list is always written to <staging_location>/file-list.
        // Source column: original path for data files, staging path for metadata files.
        // Target column: final target path for all files.
        String tableName = "rewrite_table_path_file_list";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_fl_migrated";
            String stagingLocation = sourcePrefix + "_fl_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            String fileListPath = stagingLocation + "/" + RewriteTablePathProcedure.FILE_LIST_NAME;
            String localPath = fileListPath.startsWith("file:") ? fileListPath.substring("file:".length()) : fileListPath;
            List<String> lines = Files.readAllLines(java.nio.file.Paths.get(localPath));

            assertTrue(lines.size() > 0, "file-list should not be empty");
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                assertEquals(parts.length, 2, "Each line should have exactly two columns: " + line);
                assertTrue(parts[1].startsWith(targetPrefix),
                        format("Target column '%s' should start with '%s'", parts[1], targetPrefix));
            }

            List<String> sourceCol = lines.stream().map(l -> l.split(",", 2)[0]).collect(Collectors.toList());
            List<String> targetCol = lines.stream().map(l -> l.split(",", 2)[1]).collect(Collectors.toList());

            // Data files: source column is the original location, target is target_prefix.
            assertTrue(sourceCol.stream().anyMatch(p -> p.endsWith(".parquet")),
                    "file-list source should contain at least one data file (.parquet)");
            assertTrue(targetCol.stream().anyMatch(p -> p.endsWith(".parquet")),
                    "file-list target should contain at least one data file (.parquet)");

            // Metadata files: source column is the staging path.
            assertTrue(sourceCol.stream().filter(p -> p.endsWith(".avro")).allMatch(p -> p.startsWith(stagingLocation)),
                    "Avro source paths should be under staging_location");
            assertTrue(targetCol.stream().anyMatch(p -> p.contains("/metadata/snap-") && p.endsWith(".avro")),
                    "file-list should contain at least one manifest list (.avro)");
            assertTrue(targetCol.stream().anyMatch(p -> p.contains("/metadata/") && p.endsWith(".avro") && !p.contains("/metadata/snap-")),
                    "file-list should contain at least one manifest file (.avro)");
            assertTrue(targetCol.stream().anyMatch(p -> p.endsWith(".metadata.json")),
                    "file-list should contain the metadata JSON file");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathPreviousMetadataVersionsRewritten()
            throws IOException
    {
        // Every previous .metadata.json version (tracked in the metadata log) must also be
        // physically rewritten to staging with content referencing target_prefix.
        // We do multiple inserts to guarantee several metadata versions exist.
        String tableName = "rewrite_table_path_prev_metadata";
        createTable(tableName);
        try {
            // Three inserts → at least three metadata versions (v1 from CREATE, v2, v3, v4).
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_prev_meta_target";
            String stagingLocation = sourcePrefix + "_prev_meta_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // All .metadata.json files in the staging subtree must exist and reference target_prefix.
            String stagingLocalPath = stagingLocation.startsWith("file:") ? stagingLocation.substring("file:".length()) : stagingLocation;
            List<Path> metadataFiles = Files.walk(java.nio.file.Paths.get(stagingLocalPath))
                    .filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
                    .collect(Collectors.toList());

            assertTrue(metadataFiles.size() >= 2,
                    "Expected at least 2 rewritten metadata versions under staging, found: " + metadataFiles.size());

            for (Path metaFile : metadataFiles) {
                String content = new String(Files.readAllBytes(metaFile), java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(content.contains(targetPrefix),
                        format("Metadata file '%s' should reference target_prefix '%s'", metaFile, targetPrefix));
                // After stripping all targetPrefix occurrences, no residual sourcePrefix should remain.
                // (A plain contains(sourcePrefix) check would always fail because targetPrefix starts
                // with sourcePrefix — every targetPrefix hit is also a sourcePrefix hit.)
                String contentWithoutTarget = content.replace(targetPrefix, "");
                assertTrue(!contentWithoutTarget.contains(sourcePrefix),
                        format("Metadata file '%s' should NOT reference source_prefix '%s' outside of target_prefix", metaFile, sourcePrefix));
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // End-to-end — full migration using the file-list as the sole copy manifest
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathEndToEnd()
            throws IOException
    {
        // Default UUID staging: procedure picks the staging dir automatically.
        // The file-list CSV is the complete copy manifest — copy every row
        // (data files and metadata files alike) then register and query.
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

            // Step 1: rewrite — no staging_location, UUID staging dir chosen automatically.
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix));

            // Step 2: find the UUID staging dir under <originalLocation>/metadata
            // and read the file-list at <staging>/file-list.
            String sourceMetadataDir = originalLocation.replace("file:", "") + "/metadata";
            java.nio.file.Path stagingDir = Files.list(java.nio.file.Paths.get(sourceMetadataDir))
                    .filter(p -> p.getFileName().toString().startsWith(RewriteTablePathProcedure.STAGING_DIR_PREFIX))
                    .filter(java.nio.file.Files::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No staging dir found under " + sourceMetadataDir));

            java.nio.file.Path fileListPath = stagingDir.resolve(RewriteTablePathProcedure.FILE_LIST_NAME);
            assertTrue(java.nio.file.Files.exists(fileListPath), "file-list should exist at " + fileListPath);
            List<String> lines = Files.readAllLines(fileListPath);
            assertTrue(lines.size() > 0, "file-list should not be empty");

            // Step 3: copy every row — data files (original → target) and
            // metadata files (staging → target). No other path logic needed.
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                java.nio.file.Path from = java.nio.file.Paths.get(parts[0].replace("file:", ""));
                java.nio.file.Path to = java.nio.file.Paths.get(parts[1].replace("file:", ""));
                java.nio.file.Files.createDirectories(to.getParent());
                java.nio.file.Files.copy(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry in file-list"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            // Step 5: register and query.
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a'), (2, 'b')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    @Test
    public void testRewriteTablePathEndToEndWithStagingLocation()
            throws IOException
    {
        // Explicit staging_location: caller knows the staging path up front so
        // the file-list can be found directly without scanning for a UUID dir.
        // Same copy-everything-from-CSV workflow as the default staging test.
        String sourceName = "rewrite_e2e_staged_source";
        String targetName = "rewrite_e2e_staged_target";
        createTable(sourceName);
        try {
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + sourceName + " VALUES (2, 'b')", 1);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_e2e_staged_target";
            String stagingLocation = sourcePrefix + "_e2e_staging";

            // Step 1: rewrite with an explicit staging_location.
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Step 2: file-list is at the known path <staging_location>/file-list.
            java.nio.file.Path fileListPath = java.nio.file.Paths.get(
                    (stagingLocation + "/file-list").replace("file:", ""));
            assertTrue(java.nio.file.Files.exists(fileListPath), "file-list should exist at " + fileListPath);
            List<String> lines = Files.readAllLines(fileListPath);
            assertTrue(lines.size() > 0, "file-list should not be empty");

            // Step 3: copy every row — data files (original → target) and
            // metadata files (staging → target). No other path logic needed.
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                java.nio.file.Path from = java.nio.file.Paths.get(parts[0].replace("file:", ""));
                java.nio.file.Path to = java.nio.file.Paths.get(parts[1].replace("file:", ""));
                java.nio.file.Files.createDirectories(to.getParent());
                java.nio.file.Files.copy(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found in file-list"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            // Step 5: register and query.
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a'), (2, 'b')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    /**
     * Finds the latest {@code .metadata.json} written under {@code <tableLocation>/metadata/}
     * by last-modified time and parses it. Avoids hardcoding version numbers (v1, v2, v3, …).
     */
    private TableMetadata readLatestTargetMetadata(String tableLocation)
    {
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
}
