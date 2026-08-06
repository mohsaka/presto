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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergLibUtils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.PositionDeleteReaderWriter;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.Pair;

import javax.inject.Provider;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.MIN_FORMAT_VERSION_FOR_ROW_LINEAGE;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.ManifestContent.DELETES;
import static org.apache.iceberg.RewriteTablePathUtil.maybeAppendFileSeparator;
import static org.apache.iceberg.RewriteTablePathUtil.newPath;
import static org.apache.iceberg.RewriteTablePathUtil.replacePaths;
import static org.apache.iceberg.RewriteTablePathUtil.rewriteDataManifest;
import static org.apache.iceberg.RewriteTablePathUtil.rewriteDeleteManifest;
import static org.apache.iceberg.RewriteTablePathUtil.rewritePositionDeleteFile;
import static org.apache.iceberg.RewriteTablePathUtil.stagingPath;
import static org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader;
import static org.apache.iceberg.io.DeleteSchemaUtil.posDeleteReadSchema;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

/**
 * Coordinator-only procedure that rewrites an Iceberg table's metadata files (table metadata JSON,
 * manifest lists, and manifests) into a new location by substituting {@code source_prefix} with
 * {@code target_prefix} in every embedded path. Source files are left untouched and the catalog is
 * not updated; the caller copies the files named in the generated file list and then registers the
 * table at the new location. See the {@code rewrite_table_path} section of the Iceberg connector
 * documentation for the user-facing contract.
 *
 * <p>Per-file rewriting is delegated to Iceberg's own {@link RewriteTablePathUtil}, so every file
 * emitted here is produced by the writers Iceberg uses everywhere else: manifests and position
 * deletes through {@code RewriteTablePathUtil}, metadata JSON through {@link TableMetadataParser},
 * and manifest lists through Iceberg's {@code ManifestLists} writer reached via
 * {@link IcebergLibUtils}. All I/O goes through Presto's {@link HdfsFileIO}. What this class adds
 * is the orchestration {@code RewriteTablePathUtil} does not provide: argument validation,
 * version-range selection, the copy plan, and the manifest length correction below.
 *
 * <p><strong>Manifest length:</strong> Rewriting a manifest changes its byte size, because the
 * embedded paths grow or shrink. Iceberg records each manifest's length in the manifest list and
 * both Iceberg and Presto pass that value to the filesystem as a hard read limit (see
 * {@link org.apache.iceberg.io.FileIO#newInputFile(ManifestFile)} and {@link HdfsFileIO}), so a
 * stale length truncates the read and silently drops manifest entries. Manifests are therefore
 * rewritten first and their actual sizes recorded, then written into the manifest lists.
 */
public class RewriteTablePathProcedure
        implements Provider<Procedure>
{
    // visible for testing
    public static final String STAGING_DIR_PREFIX = "copy-table-staging-";
    public static final String FILE_LIST_NAME = "file-list";

    // Field positions in GenericManifestFile, which is mutated positionally through StructLike
    // because ManifestFile exposes no setters.
    private static final int MANIFEST_LIST_PATH_POSITION = 0;
    private static final int MANIFEST_LIST_LENGTH_POSITION = 1;

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
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, session, new SchemaTableName(schema, tableName));
            TableMetadata currentMetadata = ((BaseTable) icebergTable).operations().current();
            String currentLocation = icebergTable.location();

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);
            RewriteContext context = validatedContext(
                    fileIO, currentMetadata, currentLocation, sourcePrefix, targetPrefix, stagingLocation);

            List<String> metadataFiles = metadataLog(currentMetadata);
            int startIndex = startVersion == null ? 0 : findMetadataVersionIndex(metadataFiles, startVersion);
            int endIndex = endVersion == null
                    ? metadataFiles.size() - 1
                    : findMetadataVersionIndex(metadataFiles, endVersion);
            if (startIndex > endIndex) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                        "start_version '%s' is chronologically after end_version '%s'",
                        startVersion, endVersion));
            }

            TableMetadata endMetadata = readMetadata(metadataFiles.get(endIndex), fileIO);
            Set<Long> deltaSnapshotIds = deltaSnapshotIds(
                    startVersion != null ? readMetadata(metadataFiles.get(startIndex), fileIO) : null,
                    endMetadata);
            Map<Integer, PartitionSpec> specsById = endMetadata.specsById();
            CopyPlan copyPlan = new CopyPlan(context);

            // Step 1: rewrite the manifests, collecting both the content file copy plan and the
            // position delete files step 3 has to rewrite. Keyed by source location: a delete file
            // can be referenced from more than one delete manifest, and rewriting it twice would
            // fail on the already-staged output file.
            Map<String, DeleteFile> positionDeletesToRewrite = new LinkedHashMap<>();
            Map<String, Long> rewrittenManifestLengths = rewriteManifests(
                    endMetadata, deltaSnapshotIds, specsById, context, copyPlan, positionDeletesToRewrite);

            // Step 2: rewrite the manifest lists, whose entries reference the manifests from step 1.
            rewriteManifestLists(endMetadata, context, rewrittenManifestLengths, copyPlan);

            // Step 3: rewrite the position delete files discovered in step 1.
            for (DeleteFile deleteFile : positionDeletesToRewrite.values()) {
                rewritePositionDelete(deleteFile, specsById, context);
            }

            // Step 4: rewrite the metadata JSON files in the requested version window.
            for (int i = startIndex; i <= endIndex; i++) {
                copyPlan.addAll(rewriteMetadataJson(metadataFiles.get(i), context));
            }

            if (createFileList) {
                copyPlan.writeCsv(context.stagingPrefix() + "/" + FILE_LIST_NAME);
            }
        }
    }

    /**
     * Validates the prefix arguments and returns the context the rewrite steps share. The order of
     * the checks is what decides which error a bad call reports, so it is fixed: a prefix that
     * normalizes to nothing, then prefixes that do not differ, then a table the source prefix does
     * not cover.
     */
    private static RewriteContext validatedContext(
            FileIO fileIO,
            TableMetadata currentMetadata,
            String tableLocation,
            String sourcePrefix,
            String targetPrefix,
            String stagingLocation)
    {
        String normalizedSource = normalizePrefixArgument(sourcePrefix, "source_prefix");
        String normalizedTarget = normalizePrefixArgument(targetPrefix, "target_prefix");

        if (normalizedSource.equals(normalizedTarget)) {
            throw new PrestoException(ICEBERG_INVALID_METADATA,
                    "source_prefix and target_prefix must differ");
        }
        if (!tableLocation.startsWith(normalizedSource)) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                    "Table location '%s' does not start with source prefix '%s'",
                    tableLocation, normalizedSource));
        }

        String normalizedStaging = stagingLocation == null
                ? defaultStagingLocation(currentMetadata)
                : normalizePrefixArgument(stagingLocation, "staging_location");
        // Staging must differ from source, or the source files would be overwritten.
        if (normalizedStaging.equals(normalizedSource)) {
            throw new PrestoException(ICEBERG_INVALID_METADATA,
                    "staging_location must differ from source_prefix");
        }
        return new RewriteContext(fileIO, normalizedSource, normalizedTarget, normalizedStaging);
    }

    /**
     * Strips the trailing slash from a prefix argument, rejecting a value that normalizes to
     * nothing. Checked before {@link org.apache.iceberg.util.LocationUtil#stripTrailingSlash}, which
     * rejects null and empty with its own {@link IllegalArgumentException}. An empty prefix would
     * also match between every character of every path.
     */
    private static String normalizePrefixArgument(String value, String argumentName)
    {
        if (isNullOrEmpty(value) || stripTrailingSlash(value).isEmpty()) {
            throw new PrestoException(ICEBERG_INVALID_METADATA,
                    format("%s cannot be empty or '/'", argumentName));
        }
        return stripTrailingSlash(value);
    }

    /**
     * The table's metadata files, oldest to newest: the metadata log followed by the current
     * metadata file.
     */
    private static List<String> metadataLog(TableMetadata currentMetadata)
    {
        List<String> metadataFiles = new ArrayList<>();
        for (MetadataLogEntry entry : currentMetadata.previousFiles()) {
            metadataFiles.add(entry.file());
        }
        metadataFiles.add(currentMetadata.metadataFileLocation());
        return metadataFiles;
    }

    /**
     * The snapshots in {@code endMetadata} that {@code startMetadata} does not have. Data files
     * added by these snapshots are the ones that need copying; the rest are assumed to be at the
     * target already. In full-migration mode {@code startMetadata} is null, so every snapshot in
     * end metadata is in the delta.
     */
    private static Set<Long> deltaSnapshotIds(TableMetadata startMetadata, TableMetadata endMetadata)
    {
        Set<Long> startSnapshotIds = startMetadata != null
                ? startMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(toSet())
                : emptySet();
        return endMetadata.snapshots().stream()
                .map(Snapshot::snapshotId)
                .filter(id -> !startSnapshotIds.contains(id))
                .collect(toSet());
    }

    /**
     * Rewrites every manifest reachable from {@code endMetadata} into the staging location, adding
     * the content files that need copying to {@code copyPlan} and the position delete files that
     * need rewriting to {@code positionDeletesToRewrite}. Manifests are shared across snapshots, so
     * each one is rewritten once. Every entry is written to the rewritten manifest, but only live
     * entries created by a snapshot in {@code deltaSnapshotIds} enter the copy plan.
     *
     * @return each rewritten manifest's actual length, keyed by its final target path, which is
     * what the manifest lists written by {@link #rewriteManifestLists} reference
     */
    private static Map<String, Long> rewriteManifests(
            TableMetadata endMetadata,
            Set<Long> deltaSnapshotIds,
            Map<Integer, PartitionSpec> specsById,
            RewriteContext context,
            CopyPlan copyPlan,
            Map<String, DeleteFile> positionDeletesToRewrite)
    {
        Map<String, Long> rewrittenManifestLengths = new HashMap<>();
        Set<String> rewrittenManifests = new LinkedHashSet<>();
        for (Snapshot snapshot : endMetadata.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(context.fileIO())) {
                if (!rewrittenManifests.add(manifest.path())) {
                    continue;
                }
                context.checkUnderSourcePrefix("Manifest path", manifest.path());
                String manifestStagingPath = context.stagingPathFor(manifest.path());
                String manifestFinalPath = context.targetPathFor(manifest.path());
                OutputFile manifestOutput = context.fileIO().newOutputFile(manifestStagingPath);

                try {
                    if (manifest.content() == DELETES) {
                        RewriteResult<DeleteFile> result = rewriteDeleteManifest(
                                manifest, deltaSnapshotIds, manifestOutput, context.fileIO(),
                                endMetadata.formatVersion(), specsById, context.sourcePrefix(),
                                context.targetPrefix(), context.stagingPrefix());
                        copyPlan.addAll(result.copyPlan());
                        // Position delete payloads embed absolute data file paths, so those files
                        // must be rewritten rather than merely copied.
                        result.toRewrite().stream()
                                .filter(file -> file.content() == POSITION_DELETES)
                                .forEach(file -> positionDeletesToRewrite.putIfAbsent(
                                        file.location(), file));
                    }
                    else {
                        RewriteResult<DataFile> result = rewriteDataManifest(
                                manifest, deltaSnapshotIds, manifestOutput, context.fileIO(),
                                endMetadata.formatVersion(), specsById, context.sourcePrefix(),
                                context.targetPrefix());
                        copyPlan.addAll(result.copyPlan());
                    }
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                            format("Failed to rewrite manifest '%s'", manifest.path()), e);
                }

                rewrittenManifestLengths.put(
                        manifestFinalPath,
                        context.fileIO().newInputFile(manifestStagingPath).getLength());
                copyPlan.add(manifestStagingPath, manifestFinalPath);
            }
        }
        return rewrittenManifestLengths;
    }

    /**
     * Rewrites the manifest list of every snapshot in {@code endMetadata}, not just the delta — each
     * snapshot has its own list and all of them must be present for the end version to be readable.
     */
    private static void rewriteManifestLists(
            TableMetadata endMetadata,
            RewriteContext context,
            Map<String, Long> rewrittenManifestLengths,
            CopyPlan copyPlan)
    {
        Set<String> rewrittenManifestLists = new LinkedHashSet<>();
        for (Snapshot snapshot : endMetadata.snapshots()) {
            String manifestListPath = snapshot.manifestListLocation();
            if (manifestListPath == null || !rewrittenManifestLists.add(manifestListPath)) {
                continue;
            }
            context.checkUnderSourcePrefix("Manifest list path", manifestListPath);
            String listStagingPath = context.stagingPathFor(manifestListPath);

            writeManifestList(snapshot, endMetadata, context, listStagingPath, rewrittenManifestLengths);
            copyPlan.add(listStagingPath, context.targetPathFor(manifestListPath));
        }
    }

    /**
     * Writes the manifest list for {@code snapshot} with every path replaced and every
     * {@code manifest_length} corrected to the rewritten manifest's actual size.
     *
     * <p>{@link RewriteTablePathUtil#rewriteManifestList} is not used because it replaces only
     * {@code manifest_path}, leaving each entry's length field holding the <em>source</em>
     * manifest's size. Both fields have to change together, so the entries are prepared here and
     * handed to Iceberg's own manifest list writer via {@link IcebergLibUtils}.
     */
    private static void writeManifestList(
            Snapshot snapshot,
            TableMetadata endMetadata,
            RewriteContext context,
            String outputPath,
            Map<String, Long> rewrittenManifestLengths)
    {
        // A table upgraded in place to v3 keeps the snapshots it took while it was v1 or v2, and
        // those have no first_row_id because row lineage did not exist yet. Iceberg's v3 manifest
        // list writer takes a primitive first row id, so such a snapshot is written with the
        // highest writer that predates row lineage — the same way the source table already stores
        // it. Inventing an id instead would assign row ids the source never had.
        int manifestListFormatVersion = endMetadata.formatVersion();
        if (manifestListFormatVersion >= MIN_FORMAT_VERSION_FOR_ROW_LINEAGE && snapshot.firstRowId() == null) {
            manifestListFormatVersion = MIN_FORMAT_VERSION_FOR_ROW_LINEAGE - 1;
        }

        try (FileAppender<ManifestFile> writer = IcebergLibUtils.writeManifestList(
                manifestListFormatVersion,
                context.fileIO().newOutputFile(outputPath),
                snapshot.snapshotId(),
                snapshot.parentId(),
                snapshot.sequenceNumber(),
                snapshot.firstRowId())) {
            // allManifests reads the source manifest list, which step 1 has already cached on the
            // snapshot, so this does not re-read it.
            for (ManifestFile manifest : snapshot.allManifests(context.fileIO())) {
                context.checkUnderSourcePrefix("Manifest path", manifest.path());
                String targetPath = context.targetPathFor(manifest.path());
                ManifestFile newManifest = manifest.copy();
                ((StructLike) newManifest).set(MANIFEST_LIST_PATH_POSITION, targetPath);
                Long rewrittenLength = rewrittenManifestLengths.get(targetPath);
                if (rewrittenLength != null) {
                    ((StructLike) newManifest).set(MANIFEST_LIST_LENGTH_POSITION, rewrittenLength);
                }
                writer.add(newManifest);
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to rewrite manifest list '%s'", snapshot.manifestListLocation()), e);
        }
    }

    /**
     * Rewrites a position delete file into the staging location, replacing the absolute data file
     * paths embedded in its payload. Avro and Parquet are supported; other formats are rejected by
     * {@link IcebergPositionDeleteReaderWriter}.
     */
    private static void rewritePositionDelete(
            DeleteFile deleteFile,
            Map<Integer, PartitionSpec> specsById,
            RewriteContext context)
    {
        String deleteStagingPath = context.stagingPathFor(deleteFile.location());
        try {
            rewritePositionDeleteFile(
                    deleteFile,
                    context.fileIO().newOutputFile(deleteStagingPath),
                    context.fileIO(),
                    specsById.get(deleteFile.specId()),
                    context.sourcePrefix(),
                    context.targetPrefix(),
                    new IcebergPositionDeleteReaderWriter());
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to rewrite position delete file '%s'", deleteFile.location()), e);
        }
    }

    /**
     * Rewrites the metadata JSON at {@code sourceMetadataPath} into the staging location using
     * Iceberg's own parser, and returns the copy plan rows for the metadata file and any
     * statistics files it references.
     */
    private static Set<Pair<String, String>> rewriteMetadataJson(String sourceMetadataPath, RewriteContext context)
    {
        Set<Pair<String, String>> result = new LinkedHashSet<>();
        TableMetadata metadata = readMetadata(sourceMetadataPath, context.fileIO());
        TableMetadata rewritten = replacePaths(metadata, context.sourcePrefix(), context.targetPrefix());

        String metadataStagingPath = context.stagingPathFor(sourceMetadataPath);
        // TableMetadataParser writes no metadata-file-location field, so the staging path this
        // OutputFile points at does not leak into the file's contents.
        TableMetadataParser.overwrite(rewritten, context.fileIO().newOutputFile(metadataStagingPath));
        result.add(Pair.of(metadataStagingPath, context.targetPathFor(sourceMetadataPath)));

        // Statistics files are copied as-is; replacePaths rewrote the pointers to them inside the
        // metadata JSON.
        List<StatisticsFile> beforeStats = metadata.statisticsFiles();
        List<StatisticsFile> afterStats = rewritten.statisticsFiles();
        for (int i = 0; i < beforeStats.size(); i++) {
            result.add(Pair.of(beforeStats.get(i).path(), afterStats.get(i).path()));
        }
        return result;
    }

    /**
     * Finds the position of {@code version} in {@code metadataFiles} (ordered oldest → newest).
     * {@code version} may be a full path or a bare filename in either of the naming schemes
     * Iceberg catalogs use, sequential ({@code v2.metadata.json}) or UUID-based
     * ({@code 00002-&lt;uuid&gt;.metadata.json}).
     */
    private static int findMetadataVersionIndex(List<String> metadataFiles, String version)
    {
        for (int i = 0; i < metadataFiles.size(); i++) {
            String path = metadataFiles.get(i);
            if (path.equals(version) || path.endsWith("/" + version)) {
                return i;
            }
        }
        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                "Metadata version '%s' not found in table's metadata log", version));
    }

    /**
     * Returns the default staging directory: a UUID-named subdirectory under the source table's
     * metadata directory. This is under the source prefix, so callers migrating from a read-only
     * source must pass {@code staging_location} explicitly.
     */
    private static String defaultStagingLocation(TableMetadata sourceMetadata)
    {
        String metadataFileLocation = sourceMetadata.metadataFileLocation();
        String metadataDir = metadataFileLocation.substring(0, metadataFileLocation.lastIndexOf('/'));
        return metadataDir + "/" + STAGING_DIR_PREFIX + UUID.randomUUID();
    }

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

    /**
     * The filesystem and the three prefixes every step shares. A file is read from under
     * {@code sourcePrefix}, physically written under {@code stagingPrefix}, and refers to
     * {@code targetPrefix} in its contents, so it is correct once moved from staging to the target.
     */
    private static class RewriteContext
    {
        private final FileIO fileIO;
        private final String sourcePrefix;
        private final String targetPrefix;
        private final String stagingPrefix;

        public RewriteContext(FileIO fileIO, String sourcePrefix, String targetPrefix, String stagingPrefix)
        {
            this.fileIO = requireNonNull(fileIO, "fileIO is null");
            this.sourcePrefix = requireNonNull(sourcePrefix, "sourcePrefix is null");
            this.targetPrefix = requireNonNull(targetPrefix, "targetPrefix is null");
            this.stagingPrefix = requireNonNull(stagingPrefix, "stagingPrefix is null");
        }

        public FileIO fileIO()
        {
            return fileIO;
        }

        public String sourcePrefix()
        {
            return sourcePrefix;
        }

        public String targetPrefix()
        {
            return targetPrefix;
        }

        public String stagingPrefix()
        {
            return stagingPrefix;
        }

        /**
         * Where a file under source_prefix ends up once the caller has copied the file list.
         */
        public String targetPathFor(String sourcePath)
        {
            return newPath(sourcePath, sourcePrefix, targetPrefix);
        }

        /**
         * Where the rewritten copy of a file under source_prefix is physically written.
         */
        public String stagingPathFor(String sourcePath)
        {
            return stagingPath(sourcePath, sourcePrefix, stagingPrefix);
        }

        /**
         * Fails if {@code path} is not under source_prefix. Such a path cannot be rewritten, and
         * copying the table without it would leave the target referencing the source location.
         */
        public void checkUnderSourcePrefix(String description, String path)
        {
            if (!path.startsWith(sourcePrefix)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                        "%s '%s' does not start with source_prefix '%s'", description, path, sourcePrefix));
            }
        }
    }

    /**
     * The files to copy, from the path to copy from to the path to copy to: the staging path for a
     * rewritten file and the source path for a file copied as-is. Insertion-ordered so the file list
     * is stable across runs, and keyed by source because the same file can appear in several
     * manifest entries with different statuses; the first insertion wins.
     */
    private static class CopyPlan
    {
        private final RewriteContext context;

        // A null value means the target is a plain source_prefix → target_prefix substitution of
        // the key, so it is derived when the file list is written rather than retained. Data files
        // dominate the plan on a large table — tens of millions of entries — and storing a second
        // path per entry roughly doubles the footprint for no information.
        private final Map<String, String> entries = new LinkedHashMap<>();

        public CopyPlan(RewriteContext context)
        {
            this.context = requireNonNull(context, "context is null");
        }

        public void add(String source, String target)
        {
            // Only a source under source_prefix can be relativized against it; rewritten files are
            // copied from the staging location, which is somewhere else entirely. The separator
            // matters: a staging location of '<source>_staging' shares source_prefix as a string
            // prefix without being under that directory, and relativize would reject it.
            boolean derivable = source.startsWith(maybeAppendFileSeparator(context.sourcePrefix()))
                    && target.equals(context.targetPathFor(source));
            entries.putIfAbsent(source, derivable ? null : target);
        }

        public void addAll(Iterable<Pair<String, String>> pairs)
        {
            for (Pair<String, String> pair : pairs) {
                add(pair.first(), pair.second());
            }
        }

        /**
         * Writes a two-column CSV (no header) of {@code source,target} path pairs. Values are not
         * quoted, so a path containing a comma would produce an ambiguous row.
         *
         * <p>Rows are written as they are produced rather than accumulated: on a table with tens of
         * millions of data files the whole list runs to gigabytes, and buffering it in a
         * {@link StringBuilder} first would both triple the peak footprint and, past roughly 12
         * million rows, exceed the maximum array length no matter how much heap the coordinator has.
         * Presto's S3 filesystem streams to a local staging file and uploads on close, so this keeps
         * heap flat on object stores too.
         */
        public void writeCsv(String path)
        {
            OutputFile outputFile = context.fileIO().newOutputFile(path);
            try (PositionOutputStream out = outputFile.createOrOverwrite();
                    Writer writer = new BufferedWriter(
                            new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
                for (Map.Entry<String, String> entry : entries.entrySet()) {
                    String source = entry.getKey();
                    String target = entry.getValue() != null
                            ? entry.getValue()
                            : context.targetPathFor(source);
                    writer.write(source);
                    writer.write(',');
                    writer.write(target);
                    writer.write('\n');
                }
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                        format("Failed to write file list to '%s'", path), e);
            }
        }
    }

    /**
     * Reads and writes position delete files using Iceberg's own generic record readers and
     * writers, so a rewritten file is what Iceberg itself would have produced. ORC is not
     * supported: {@code iceberg-orc} is not a dependency of this module.
     */
    private static class IcebergPositionDeleteReaderWriter
            implements PositionDeleteReaderWriter
    {
        @Override
        public CloseableIterable<Record> reader(InputFile inputFile, FileFormat format, PartitionSpec spec)
        {
            Schema deleteSchema = posDeleteReadSchema(spec.schema());
            switch (format) {
                case AVRO:
                    return Avro.read(inputFile)
                            .project(deleteSchema)
                            .reuseContainers()
                            .createReaderFunc(DataReader::create)
                            .build();
                case PARQUET:
                    return Parquet.read(inputFile)
                            .project(deleteSchema)
                            .reuseContainers()
                            .createReaderFunc(fileSchema -> buildReader(deleteSchema, fileSchema))
                            .build();
                default:
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Cannot rewrite position delete file in format '%s'", format));
            }
        }

        @Override
        public PositionDeleteWriter<Record> writer(
                OutputFile outputFile,
                FileFormat format,
                PartitionSpec spec,
                StructLike partition,
                Schema rowSchema)
                throws IOException
        {
            switch (format) {
                case AVRO:
                    return Avro.writeDeletes(outputFile)
                            .createWriterFunc(DataWriter::create)
                            .withPartition(partition)
                            .rowSchema(rowSchema)
                            .withSpec(spec)
                            .buildPositionWriter();
                case PARQUET:
                    return Parquet.writeDeletes(outputFile)
                            .createWriterFunc(GenericParquetWriter::create)
                            .withPartition(partition)
                            .rowSchema(rowSchema)
                            .withSpec(spec)
                            .buildPositionWriter();
                default:
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Cannot rewrite position delete file in format '%s'", format));
            }
        }
    }
}
