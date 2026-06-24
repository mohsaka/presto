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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
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
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
 * directly to {@code target_prefix} (the previous default behaviour).
 *
 * <p>Rewrite scope per file type:
 * <ul>
 *   <li>Metadata JSON: full serialized JSON string replacement covers all path fields.</li>
 *   <li>Manifest list Avro: {@code manifest_path} field (top-level) in each record.</li>
 *   <li>Manifest Avro: {@code data_file.file_path} field (nested under {@code data_file})
 *       in each record.</li>
 * </ul>
 */
public class RewriteTablePathProcedure
        implements Provider<Procedure>
{
    public static final String STAGING_DIR_PREFIX = "copy-table-staging-";
    public static final String FILE_LIST_NAME = "file-list";

    private static final String MANIFEST_DATA_FILE_FIELD = "data_file";
    private static final String MANIFEST_FILE_PATH_FIELD = "file_path";
    private static final String MANIFEST_LIST_PATH_FIELD = "manifest_path";

    private static final MethodHandle REWRITE_TABLE_PATH = methodHandle(
            RewriteTablePathProcedure.class,
            "rewriteTablePath",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);

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
                        new Argument("staging_location", VARCHAR, false, null)),
                REWRITE_TABLE_PATH.bindTo(this));
    }

    public void rewriteTablePath(ConnectorSession session, String schema, String tableName, String sourcePrefix, String targetPrefix, String stagingLocation)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, session, schemaTableName);

            String normalizedSource = stripTrailingSlash(sourcePrefix);
            String normalizedTarget = stripTrailingSlash(targetPrefix);

            String currentLocation = icebergTable.location();
            if (!currentLocation.startsWith(normalizedSource)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                        "Table location '%s' does not start with source prefix '%s'",
                        currentLocation, normalizedSource));
            }

            TableMetadata sourceMetadata = ((org.apache.iceberg.BaseTable) icebergTable).operations().current();

            // When staging_location is provided, metadata files are physically written there
            // (preserving the relative suffix from source_prefix). The embedded path strings
            // inside each file still reference target_prefix, so the files are correct once
            // moved from staging to the final target location.
            // When omitted, default to a UUID-named directory under the source table's
            // metadata directory (matching the Iceberg spec behaviour).
            String normalizedStaging = stagingLocation != null
                    ? stripTrailingSlash(stagingLocation)
                    : defaultStagingLocation(sourceMetadata);

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);

            // Always collect file pairs: [stagingPath, finalTargetPath] for metadata files,
            // [sourcePath, finalTargetPath] for data files. Written to <staging>/file-list.
            List<String[]> fileList = new ArrayList<>();

            // Step 1: Rewrite each unique manifest Avro file — substitute file_path inside
            // the nested data_file struct. Manifests are shared across snapshots so we
            // deduplicate by path.
            Set<String> rewrittenManifests = new HashSet<>();
            for (Snapshot snapshot : sourceMetadata.snapshots()) {
                for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
                    if (rewrittenManifests.add(manifest.path())) {
                        collectDataFilePairs(manifest.path(), fileIO, normalizedSource, normalizedTarget, fileList);
                        String manifestStagingPath = manifest.path().replace(normalizedSource, normalizedStaging);
                        String manifestFinalPath = manifest.path().replace(normalizedSource, normalizedTarget);
                        rewriteAvroFile(manifest.path(), MANIFEST_DATA_FILE_FIELD, MANIFEST_FILE_PATH_FIELD, fileIO, normalizedSource, normalizedTarget, manifestStagingPath);
                        fileList.add(new String[] {manifestStagingPath, manifestFinalPath});
                    }
                }
            }

            // Step 2: Rewrite each unique manifest list Avro file — substitute manifest_path
            // (top-level field in each record).
            Set<String> rewrittenManifestLists = new HashSet<>();
            for (Snapshot snapshot : sourceMetadata.snapshots()) {
                String manifestListPath = snapshot.manifestListLocation();
                if (manifestListPath != null && rewrittenManifestLists.add(manifestListPath)) {
                    String manifestListStagingPath = manifestListPath.replace(normalizedSource, normalizedStaging);
                    String manifestListFinalPath = manifestListPath.replace(normalizedSource, normalizedTarget);
                    rewriteAvroFile(manifestListPath, null, MANIFEST_LIST_PATH_FIELD, fileIO, normalizedSource, normalizedTarget, manifestListStagingPath);
                    fileList.add(new String[] {manifestListStagingPath, manifestListFinalPath});
                }
            }

            // Step 3: Rewrite all metadata JSON files — current and all previous versions
            // tracked in the metadata log. Full string replacement covers the table location,
            // snapshot manifest-list pointers, and any explicit location properties.
            // Each file is physically written to staging; content references target_prefix.
            for (TableMetadata.MetadataLogEntry previousEntry : sourceMetadata.previousFiles()) {
                rewriteMetadataJson(previousEntry.file(), fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);
            }
            rewriteMetadataJson(sourceMetadata.metadataFileLocation(), fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);

            // Step 4: Always write the file list to <staging_location>/file-list.
            writeCsvFileList(fileList, normalizedStaging + "/" + FILE_LIST_NAME, fileIO);
        }
    }

    /**
     * Reads a manifest Avro file and appends one {@code [source, target]} pair per data/delete
     * file entry into {@code fileList}.
     */
    private static void collectDataFilePairs(
            String manifestPath,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            List<String[]> fileList)
    {
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(manifestPath));
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(sourceBytes),
                new GenericDatumReader<>())) {
            for (GenericRecord record : reader) {
                GenericRecord dataFile = (GenericRecord) record.get("data_file");
                if (dataFile == null) {
                    continue;
                }
                Schema.Field pathField = dataFile.getSchema().getField("file_path");
                if (pathField == null) {
                    continue;
                }
                Object value = dataFile.get(pathField.pos());
                if (value != null) {
                    String sourcePath = value.toString();
                    fileList.add(new String[] {sourcePath, sourcePath.replace(sourcePrefix, targetPrefix)});
                }
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to read manifest file for file list: '%s'", manifestPath), e);
        }
    }

    /**
     * Writes a two-column CSV (no header) of {@code source,target} file path pairs to
     * {@code path} using the given {@code fileIO}.
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
     * @param nestedRecord if non-null, the path field is inside this nested record (e.g.
     *                     {@code "data_file"} for manifest files); if null the field is top-level
     *                     (e.g. manifest list files where {@code manifest_path} is top-level)
     */
    private static void rewriteAvroFile(
            String sourcePath,
            String nestedRecord,
            String pathField,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String writePath)
    {
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourcePath));

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(sourceBytes),
                new GenericDatumReader<>())) {
            Schema schema = reader.getSchema();
            OutputFile outputFile = fileIO.newOutputFile(writePath);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                String codec = reader.getMetaString("avro.codec");
                writer.setCodec(org.apache.avro.file.CodecFactory.fromString(codec != null ? codec : "null"));
                writer.create(schema, outputFile.createOrOverwrite());
                for (GenericRecord record : reader) {
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
        GenericRecord target = nestedRecord != null ? (GenericRecord) record.get(nestedRecord) : record;
        if (target == null) {
            return;
        }
        Schema.Field field = target.getSchema().getField(pathField);
        if (field == null) {
            return;
        }
        Object value = target.get(field.pos());
        if (value != null) {
            target.put(field.pos(), new org.apache.avro.util.Utf8(value.toString().replace(sourcePrefix, targetPrefix)));
        }
    }

    /**
     * Returns the default staging directory: a UUID-named subdirectory under the source table's
     * metadata directory, matching the Iceberg spec default staging behaviour.
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
