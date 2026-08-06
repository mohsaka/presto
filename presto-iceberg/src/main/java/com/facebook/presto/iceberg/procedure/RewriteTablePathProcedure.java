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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.HdfsFileIO;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import javax.inject.Provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

/**
 * Coordinator-only procedure that rewrites all Iceberg metadata files (table metadata JSON,
 * manifest list Avro files, and manifest Avro files) to a new location by substituting
 * {@code source_prefix} with {@code target_prefix} in every path string.
 *
 * <p>Data and delete files are NOT written. The original source files are left completely
 * untouched and the catalog is NOT updated. The caller should copy the data/delete files and
 * then call {@code system.register_table} pointing at the new metadata file.
 *
 * <p>When {@code staging_location} is provided, rewritten metadata files are physically written
 * under the staging directory (preserving the relative path suffix from the source). The internal
 * path strings embedded in each file still reference {@code target_prefix}, so the files are
 * ready to use once moved from staging to the final target. When omitted, files are written
 * to a default staging directory (UUID-named under the source metadata directory).
 *
 * <p>{@code start_version} and {@code end_version} optionally bound the set of metadata JSON
 * files that are rewritten. Each value may be a bare filename or a full path. Iceberg uses two
 * filename schemes depending on the catalog: sequential ({@code v2.metadata.json}) and
 * UUID-based ({@code 00002-&lt;uuid&gt;.metadata.json}). Both are matched by filename suffix.
 * The ordered list is: all {@code previousFiles()} entries (oldest first)
 * followed by the current metadata file. Only metadata JSON files whose position falls within
 * {@code [start_version, end_version]} (inclusive) are rewritten.
 *
 * <p><strong>Incremental migration:</strong> When {@code start_version} is provided, only data
 * files created by snapshots in the delta ({@code end_version.snapshots - start_version.snapshots})
 * are included in the file list. This enables incremental migrations where data files from
 * {@code start_version} are assumed to already exist at the target location. When
 * {@code start_version} is null (full migration), all data files referenced by {@code end_version}
 * are included. Manifest lists for all snapshots in {@code end_version} are always rewritten to
 * maintain metadata consistency.
 *
 * <p><strong>Incremental migration assumption:</strong> The snapshot-ID-based delta calculation
 * assumes {@code start_version}'s snapshots have been faithfully migrated to the target and never
 * rolled back at source after the {@code start_version} checkpoint. A manifest entry's
 * {@code snapshot_id} field identifies the snapshot that added the file, not all snapshots that
 * reference it. If {@code start_version = v2} contains snapshot S1 with files F1, and
 * {@code end_version = v3} contains {S1, S2} with files {F1, F2}, then only F2 is migrated
 * (correct if F1 was already migrated). But if {@code start_version} was later rolled back
 * (creating v4 that drops S1), the assumption breaks — S1's files are skipped even though they
 * no longer exist at target.
 *
 * <p><strong>Incremental migration limitations:</strong>
 * <ul>
 *   <li>Compaction/rewrite operations ({@code rewrite_data_files}): new files are included (correct),
 *       but the old files they replaced are assumed to exist at the target, which may no longer be true
 *       if those files were deleted at source after the previous migration phase.</li>
 *   <li>Expired snapshots: if {@code start_version} references a snapshot that was later expired,
 *       the delta is artificially larger, leading to over-copying.</li>
 * </ul>
 * For tables with rollbacks, compaction, or expiration between migration phases, use full migration
 * ({@code start_version = null}) instead of incremental.
 *
 * <p>Rewrite scope per file type:
 * <ul>
 *   <li>Metadata JSON: full serialized JSON string replacement covers all path fields.</li>
 *   <li>Manifest list Avro: {@code manifest_path} field (top-level) in each record.</li>
 *   <li>Manifest Avro: {@code data_file.file_path} field (nested under {@code data_file})
 *       in each record.</li>
 * </ul>
 *
 * <p><strong>Partial failure:</strong> If an error occurs midway through rewriting, the staging
 * directory is left partially populated. A subsequent retry will overwrite conflicting files
 * deterministically (same inputs produce the same outputs), but the staging area will contain
 * a mix of files from both runs until the procedure completes successfully.
 */
public class RewriteTablePathProcedure
        implements Provider<Procedure>
{
    // visible for testing
    public static final String STAGING_DIR_PREFIX = "copy-table-staging-";
    public static final String FILE_LIST_NAME = "file-list";

    private static final String MANIFEST_DATA_FILE_FIELD = "data_file";
    private static final String MANIFEST_FILE_PATH_FIELD = "file_path";
    private static final String MANIFEST_LIST_PATH_FIELD = "manifest_path";

    private static final MethodHandle REWRITE_TABLE_PATH = methodHandle(
            RewriteTablePathProcedure.class,
            "rewriteTablePath",
            ConnectorSession.class,
            String.class,   // schema
            String.class,   // tableName
            String.class,   // sourcePrefix
            String.class,   // targetPrefix
            String.class,   // startVersion
            String.class,   // endVersion
            String.class,   // stagingLocation
            boolean.class); // createFileList

    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final ManifestFileCache manifestFileCache;

    @Inject
    public RewriteTablePathProcedure(
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment,
            ManifestFileCache manifestFileCache)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.manifestFileCache = requireNonNull(manifestFileCache, "manifestFileCache is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rewrite_table_path",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("source_prefix", VARCHAR),
                        new Argument("target_prefix", VARCHAR),
                        new Argument("start_version", VARCHAR, false, null),
                        new Argument("end_version", VARCHAR, false, null),
                        new Argument("staging_location", VARCHAR, false, null),
                        new Argument("create_file_list", BOOLEAN, false, true)),
                REWRITE_TABLE_PATH.bindTo(this));
    }

    public void rewriteTablePath(
            ConnectorSession session,
            String schema,
            String tableName,
            String sourcePrefix,
            String targetPrefix,
            String startVersion,
            String endVersion,
            String stagingLocation,
            boolean createFileList)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, session, schemaTableName);

            String normalizedSource = stripTrailingSlash(sourcePrefix);
            String normalizedTarget = stripTrailingSlash(targetPrefix);

            // Guard: source prefix must not be empty (would match between every character)
            if (normalizedSource.isEmpty()) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "source_prefix cannot be empty or '/'");
            }

            // Guard: source and target must differ
            if (normalizedSource.equals(normalizedTarget)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "source_prefix and target_prefix must differ");
            }

            String currentLocation = icebergTable.location();
            // Verify table location is under source prefix
            if (!currentLocation.startsWith(normalizedSource)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                        "Table location '%s' does not start with source prefix '%s'",
                        currentLocation, normalizedSource));
            }

            TableMetadata currentMetadata = ((BaseTable) icebergTable).operations().current();

            // When staging_location is provided, metadata files are physically written there
            // (preserving the relative suffix from source_prefix). The embedded path strings
            // inside each file still reference target_prefix, so the files are correct once
            // moved from staging to the final target location.
            // When omitted, default to a UUID-named directory under the source table's
            // metadata directory (matching the Iceberg spec behaviour).
            String normalizedStaging = stagingLocation != null
                    ? stripTrailingSlash(stagingLocation)
                    : defaultStagingLocation(currentMetadata);

            // Guard: staging must differ from source to avoid overwriting source files
            if (normalizedStaging.equals(normalizedSource)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "staging_location must differ from source_prefix");
            }

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);

            // Always collect file pairs: [stagingPath, finalTargetPath] for metadata files,
            // [sourcePath, finalTargetPath] for data files. Written to <staging>/file-list.
            List<String[]> fileList = new ArrayList<>();

            // Determine the metadata version range [start_version, end_version].
            // Build the full ordered list (oldest → newest) then slice to the requested window.
            List<String> allMetadataFiles = new ArrayList<>();
            for (TableMetadata.MetadataLogEntry entry : currentMetadata.previousFiles()) {
                allMetadataFiles.add(entry.file());
            }
            allMetadataFiles.add(currentMetadata.metadataFileLocation());

            int startIdx = 0;
            int endIdx = allMetadataFiles.size() - 1;

            if (startVersion != null) {
                startIdx = findMetadataVersionIndex(allMetadataFiles, startVersion);
            }
            if (endVersion != null) {
                endIdx = findMetadataVersionIndex(allMetadataFiles, endVersion);
            }
            if (startIdx > endIdx) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                        "start_version '%s' is chronologically after end_version '%s'",
                        startVersion, endVersion));
            }

            // Load metadata for start and end versions to determine which snapshots to process.
            // Only snapshots referenced by metadata versions in [start, end] should have their
            // manifests and data files included in the migration (matching Iceberg Spark behavior).
            TableMetadata startMetadata = startVersion != null
                    ? readMetadata(allMetadataFiles.get(startIdx), fileIO)
                    : null;
            TableMetadata endMetadata = readMetadata(allMetadataFiles.get(endIdx), fileIO);

            // Calculate delta snapshots: snapshots in end but not in start.
            Set<Long> startSnapshotIds = startMetadata != null
                    ? startMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(toSet())
                    : emptySet();
            Set<Snapshot> deltaSnapshots = endMetadata.snapshots().stream()
                    .filter(s -> !startSnapshotIds.contains(s.snapshotId()))
                    .collect(toSet());

            // Step 1: Determine which data files to include based on start_version.
            // When start_version is null (full migration): include ALL data files from end_version.
            // When start_version is provided (incremental migration): include only data files
            // created in delta snapshots (end_version.snapshots - start_version.snapshots).
            Set<Long> deltaSnapshotIds = startVersion != null
                    ? deltaSnapshots.stream().map(Snapshot::snapshotId).collect(toSet())
                    : emptySet(); // Empty set signals: include all data files

            // Step 2: Rewrite each unique manifest Avro file from end metadata.
            // Manifests are shared across snapshots so we deduplicate by path.
            // We iterate over all snapshots in endMetadata to ensure we have all manifests,
            // but data file filtering depends on whether start_version was provided.
            // Data file pairs are collected during the manifest rewrite (single I/O pass).
            Set<String> rewrittenManifests = new HashSet<>();
            for (Snapshot snapshot : endMetadata.snapshots()) {
                for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
                    if (rewrittenManifests.add(manifest.path())) {
                        if (!manifest.path().startsWith(normalizedSource)) {
                            throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                                    "Manifest path '%s' does not start with source_prefix '%s'",
                                    manifest.path(), normalizedSource));
                        }
                        String manifestStagingPath = manifest.path().replace(normalizedSource, normalizedStaging);
                        String manifestFinalPath = manifest.path().replace(normalizedSource, normalizedTarget);

                        // Both DATA and DELETE manifests use "data_file" as the nested record field name
                        // The manifest content type is stored in the Avro metadata, not in the schema
                        String nestedRecordField = MANIFEST_DATA_FILE_FIELD;

                        // Rewrite manifest and collect data/delete file pairs in a single pass
                        rewriteAvroFile(manifest.path(), nestedRecordField, MANIFEST_FILE_PATH_FIELD,
                                fileIO, normalizedSource, normalizedTarget, manifestStagingPath,
                                fileList, deltaSnapshotIds);
                        fileList.add(new String[] {manifestStagingPath, manifestFinalPath});
                    }
                }
            }

            // Step 3: Rewrite manifest list Avro files from all snapshots in end metadata
            // (not just delta), because manifest lists are tied to specific snapshots and must
            // all be present for the end version to be valid.
            Set<String> rewrittenManifestLists = new HashSet<>();
            for (Snapshot snapshot : endMetadata.snapshots()) {
                String manifestListPath = snapshot.manifestListLocation();
                if (manifestListPath != null && rewrittenManifestLists.add(manifestListPath)) {
                    if (!manifestListPath.startsWith(normalizedSource)) {
                        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                                "Manifest list path '%s' does not start with source_prefix '%s'",
                                manifestListPath, normalizedSource));
                    }
                    String manifestListStagingPath = manifestListPath.replace(normalizedSource, normalizedStaging);
                    String manifestListFinalPath = manifestListPath.replace(normalizedSource, normalizedTarget);
                    // Manifest lists don't contain data files, so pass null for collection params
                    rewriteAvroFile(manifestListPath, null, MANIFEST_LIST_PATH_FIELD, fileIO,
                            normalizedSource, normalizedTarget, manifestListStagingPath, null, null);
                    fileList.add(new String[] {manifestListStagingPath, manifestListFinalPath});
                }
            }

            // Step 4: Add statistics files (.puffin/.stats) to the file list.
            // The metadata JSON rewrite will update the paths inside the JSON, but the actual
            // stats files need to be included in the copy manifest.
            // Note: Statistics files are rarely generated in practice (requires explicit ANALYZE
            // or external tools like Spark), but we handle them for completeness.
            if (endMetadata.statisticsFiles() != null) {
                for (StatisticsFile statsFile : endMetadata.statisticsFiles()) {
                    String statsPath = statsFile.path();
                    if (statsPath.startsWith(normalizedSource)) {
                        String statsTargetPath = statsPath.replace(normalizedSource, normalizedTarget);
                        fileList.add(new String[] {statsPath, statsTargetPath});
                    }
                }
            }

            // Step 5: Rewrite metadata JSON files in the version range [start_version, end_version].
            for (int i = startIdx; i <= endIdx; i++) {
                rewriteMetadataJson(allMetadataFiles.get(i), fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);
            }

            // Step 6: Optionally write the file list to <staging_location>/file-list.
            if (createFileList) {
                writeCsvFileList(fileList, normalizedStaging + "/" + FILE_LIST_NAME, fileIO);
            }
        }
    }

    /**
     * Writes a two-column CSV (no header) of {@code source,target} file path pairs to
     * {@code path} using the given {@code fileIO}.
     *
     * <p><strong>Note:</strong> Values are not quoted. This assumes paths do not contain commas,
     * which is true for S3/HDFS/GCS but may not hold for local filesystem paths or unusual configs.
     */
    private static void writeCsvFileList(List<String[]> fileList, String path, FileIO fileIO)
    {
        StringBuilder csv = new StringBuilder();
        for (String[] pair : fileList) {
            csv.append(pair[0]).append(',').append(pair[1]).append('\n');
        }
        byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
        OutputFile outputFile = fileIO.newOutputFile(path);
        try (PositionOutputStream out = outputFile.createOrOverwrite()) {
            out.write(bytes);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to write file list to '%s'", path), e);
        }
    }

    /**
     * Reads the Avro container file at {@code sourcePath}, rewrites the string field identified
     * by {@code pathField} (optionally nested inside a record field named {@code nestedRecord}),
     * and writes the result to {@code writePath}. The embedded path strings are rewritten from
     * {@code sourcePrefix} to {@code targetPrefix} regardless of where the file is physically
     * written (supporting the staging-location pattern).
     *
     * <p>Optionally collects data file pairs while processing manifest files (when
     * {@code collectDataFiles} is non-null), avoiding a second I/O pass.
     *
     * @param nestedRecord if non-null, the path field is inside this nested record (e.g.
     *                     {@code "data_file"} for manifest files); if null the field is top-level
     *                     (e.g. manifest list files where {@code manifest_path} is top-level)
     * @param collectDataFiles if non-null, data file paths are collected into this list during
     *                         the rewrite pass (manifest files only)
     * @param deltaSnapshotIds if non-null and non-empty, only data files from these snapshots
     *                         are collected (incremental mode); if null or empty, all data files
     *                         are collected (full migration mode)
     */
    private static void rewriteAvroFile(
            String sourcePath,
            String nestedRecord,
            String pathField,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String writePath,
            List<String[]> collectDataFiles,
            Set<Long> deltaSnapshotIds)
    {
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourcePath));

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(sourceBytes),
                new GenericDatumReader<>())) {
            Schema schema = reader.getSchema();
            OutputFile outputFile = fileIO.newOutputFile(writePath);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                // Preserve codec
                String codec = reader.getMetaString("avro.codec");
                writer.setCodec(CodecFactory.fromString(codec != null ? codec : "null"));

                // Preserve specific Iceberg metadata keys that are safe to copy.
                // These include schema and partition-spec which are necessary for correct
                // interpretation of manifests when the table has undergone schema/partition evolution.
                // Avoid copying keys that may be location-specific or auto-generated.
                // Based on org.apache.iceberg.avro.AvroFileAppender metadata keys.
                for (String key : new String[]{"format-version", "content", "partition-spec-id", "schema", "partition-spec"}) {
                    byte[] value = reader.getMeta(key);
                    if (value != null) {
                        writer.setMeta(key, value);
                    }
                }

                writer.create(schema, outputFile.createOrOverwrite());

                // Collect file paths from both data and delete manifests
                boolean shouldCollectFiles = collectDataFiles != null &&
                        MANIFEST_DATA_FILE_FIELD.equals(nestedRecord);
                boolean isIncrementalMode = deltaSnapshotIds != null && !deltaSnapshotIds.isEmpty();

                // Cache snapshot_id field lookup (avoids linear schema scan per record)
                Schema.Field snapshotIdField = isIncrementalMode && shouldCollectFiles
                        ? schema.getField("snapshot_id")
                        : null;

                for (GenericRecord record : reader) {
                    // Collect data/delete file pairs if requested (manifest files only)
                    if (shouldCollectFiles) {
                        boolean shouldIncludeInFileList = true;

                        // Filter by snapshot_id in incremental mode
                        if (isIncrementalMode) {
                            if (snapshotIdField == null) {
                                // In incremental mode, don't add entries with no snapshot_id field to file list
                                shouldIncludeInFileList = false;
                            }
                            else {
                                Object snapshotIdValue = record.get(snapshotIdField.pos());
                                if (snapshotIdValue == null) {
                                    // In incremental mode, don't add entries with null snapshot_id to file list
                                    shouldIncludeInFileList = false;
                                }
                                else {
                                    Long snapshotId = (Long) snapshotIdValue;
                                    if (!deltaSnapshotIds.contains(snapshotId)) {
                                        // Don't add files from snapshots before start_version to file list
                                        shouldIncludeInFileList = false;
                                    }
                                }
                            }
                        }

                        // Extract data/delete file path and add to collection if it passes the filter
                        if (shouldIncludeInFileList) {
                            // Check if the nested record field exists in the schema
                            Schema.Field nestedField = schema.getField(nestedRecord);
                            if (nestedField != null) {
                                GenericRecord fileRecord = (GenericRecord) record.get(nestedField.pos());
                                if (fileRecord != null) {
                                    Schema.Field filePathField = fileRecord.getSchema().getField(MANIFEST_FILE_PATH_FIELD);
                                    if (filePathField != null) {
                                        Object filePathValue = fileRecord.get(filePathField.pos());
                                        if (filePathValue != null) {
                                            String filePath = filePathValue.toString();
                                            collectDataFiles.add(new String[] {
                                                    filePath,
                                                    filePath.replace(sourcePrefix, targetPrefix)
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Always rewrite and write all records to the output manifest
                    rewritePathField(record, nestedRecord, pathField, sourcePrefix, targetPrefix);
                    writer.append(record);
                }
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to rewrite Avro file '%s' -> '%s'", sourcePath, writePath), e);
        }
    }

    /**
     * Rewrites the path string in {@code record.nestedRecord.pathField} (or
     * {@code record.pathField} when {@code nestedRecord} is null).
     */
    private static void rewritePathField(GenericRecord record, String nestedRecord, String pathField, String sourcePrefix, String targetPrefix)
    {
        GenericRecord target;
        if (nestedRecord != null) {
            // Check if the nested field exists in the schema before accessing
            Schema.Field nestedField = record.getSchema().getField(nestedRecord);
            if (nestedField == null) {
                return; // Field doesn't exist in this record's schema
            }
            target = (GenericRecord) record.get(nestedField.pos());
            if (target == null) {
                return;
            }
        }
        else {
            target = record;
        }

        Schema.Field field = target.getSchema().getField(pathField);
        if (field == null) {
            return;
        }
        Object value = target.get(field.pos());
        if (value != null) {
            target.put(field.pos(), new Utf8(value.toString().replace(sourcePrefix, targetPrefix)));
        }
    }

    /**
     * Finds the position of {@code version} in {@code metadataFiles} (ordered oldest → newest).
     * {@code version} may be a bare filename or a full path. Iceberg uses two filename schemes:
     * sequential (e.g. {@code v2.metadata.json}) and UUID-based (e.g.
     * {@code 00002-575ea024-3812-4e69-ac1e-9c8f284442e2.metadata.json}). Both are matched by
     * testing whether the stored path equals {@code version} or ends with {@code "/" + version}.
     * Throws {@link PrestoException} if no match is found.
     */
    private static int findMetadataVersionIndex(List<String> metadataFiles, String version)
    {
        for (int i = 0; i < metadataFiles.size(); i++) {
            String path = metadataFiles.get(i);
            // Match either the full path or just the filename component.
            if (path.equals(version) || path.endsWith("/" + version)) {
                return i;
            }
        }
        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                "Metadata version '%s' not found in table's metadata log", version));
    }

    /**
     * Returns the default staging directory: a UUID-named subdirectory under the source table's
     * metadata directory, matching the Iceberg spec default staging behaviour.
     *
     * <p><strong>Note:</strong> The default staging location is under the source prefix.
     * For cross-bucket migrations or when the source is read-only, the caller should explicitly
     * provide {@code staging_location} pointing to a writable location (typically under the
     * target prefix).
     */
    private static String defaultStagingLocation(TableMetadata sourceMetadata)
    {
        String metadataFileLocation = sourceMetadata.metadataFileLocation();
        String metadataDir = metadataFileLocation.substring(0, metadataFileLocation.lastIndexOf('/'));
        return metadataDir + "/" + STAGING_DIR_PREFIX + UUID.randomUUID();
    }

    /**
     * Reads the metadata JSON at {@code sourceMetadataPath}, rewrites all path strings from
     * {@code sourcePrefix} to {@code targetPrefix}, and physically writes the raw rewritten
     * bytes directly to {@code stagingPath}. Writing the raw bytes (rather than re-serialising
     * through {@code TableMetadataParser.write}) ensures the file exactly mirrors what the
     * caller requested — in particular, {@code TableMetadataParser.write} would embed the
     * {@code OutputFile.location()} (i.e. the staging path) as the {@code metadataFileLocation}
     * field, which would put the staging path back into the content.
     *
     * <p>The staging path is computed as:
     * {@code sourceMetadataPath.replace(sourcePrefix, stagingPrefix)}.
     * The final target path is:
     * {@code sourceMetadataPath.replace(sourcePrefix, targetPrefix)}.
     *
     * <p>Both paths are appended as a {@code [stagingPath, finalTargetPath]} row to
     * {@code fileList}.
     *
     * <p><strong>Limitation:</strong> Uses unanchored {@code String.replace(sourcePrefix, targetPrefix)}
     * over the entire JSON document. This rewrites all occurrences of {@code sourcePrefix}, including:
     * <ul>
     *   <li>Intended path fields: {@code location}, {@code metadata-file-location}, {@code manifest-list},
     *       {@code manifest-path}, {@code statistics}, snapshot manifest lists, etc.</li>
     *   <li>Unintended non-path fields: table properties, column comments, partition spec defaults,
     *       or any user-supplied metadata that happens to contain the prefix as a substring.</li>
     * </ul>
     * For whole-bucket migrations (e.g., {@code s3://source-bucket → s3://target-bucket}) this is typically
     * correct. For parent-directory prefixes (e.g., {@code s3://bucket/warehouse → s3://bucket/warehouse2})
     * embedded in property values, this may rewrite unintended fields. Alternative: parse JSON and rewrite
     * only known path fields (as Iceberg's Spark RewriteTablePathAction does). This implementation prioritizes
     * simplicity and handles the common case correctly.
     */
    private static void rewriteMetadataJson(
            String sourceMetadataPath,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String stagingPrefix,
            List<String[]> fileList)
    {
        // Read and rewrite the raw JSON text — a single string replace covers all path fields
        // (table location, metadataFileLocation, snapshot manifest-list pointers,
        // metadata-log entries, statistics file paths, etc.).
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourceMetadataPath));
        String rewrittenJson = new String(sourceBytes, StandardCharsets.UTF_8)
                .replace(sourcePrefix, targetPrefix);

        // Compute staging path (physical write destination) and final target path (logical).
        String stagingPath = sourceMetadataPath.replace(sourcePrefix, stagingPrefix);
        String finalTargetPath = sourceMetadataPath.replace(sourcePrefix, targetPrefix);

        // Write the rewritten bytes directly — bypassing TableMetadataParser.write() which
        // would re-serialise the object and embed the staging OutputFile.location() as the
        // metadataFileLocation, thereby reintroducing the staging path into the content.
        byte[] rewrittenBytes = rewrittenJson.getBytes(StandardCharsets.UTF_8);
        OutputFile outputFile = fileIO.newOutputFile(stagingPath);
        try (PositionOutputStream out = outputFile.createOrOverwrite()) {
            out.write(rewrittenBytes);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to write rewritten metadata JSON to '%s'", stagingPath), e);
        }

        fileList.add(new String[] {stagingPath, finalTargetPath});
    }

    /**
     * Reads and parses a metadata JSON file from the given path using the Iceberg
     * TableMetadataParser. Returns the parsed TableMetadata object.
     */
    private static TableMetadata readMetadata(String metadataPath, FileIO fileIO)
    {
        try {
            return TableMetadataParser.read(fileIO, metadataPath);
        }
        catch (Exception e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to read metadata file '%s'", metadataPath), e);
        }
    }

    private static byte[] readAllBytes(InputFile inputFile)
    {
        try (SeekableInputStream stream = inputFile.newStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = stream.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return buffer.toByteArray();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to read file '%s'", inputFile.location()), e);
        }
    }
}
