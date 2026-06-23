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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

/**
 * Coordinator-only procedure that writes a new Iceberg table metadata JSON at the target location,
 * substituting every occurrence of {@code source_prefix} with {@code target_prefix} in the
 * serialized metadata. This rewrites the table location, snapshot manifest-list pointers, and
 * any explicit location properties in one pass.
 *
 * <p>Only the metadata JSON file is written. Manifest list files, manifest files, and data files
 * are not touched. The original source metadata is left completely untouched and the catalog is
 * NOT updated. The caller is expected to copy the remaining files and then call
 * {@code system.register_table} pointing at the new metadata file.
 */
public class RewriteTablePathProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REWRITE_TABLE_PATH = methodHandle(
            RewriteTablePathProcedure.class,
            "rewriteTablePath",
            ConnectorSession.class,
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
                        new Argument("target_prefix", VARCHAR)),
                REWRITE_TABLE_PATH.bindTo(this));
    }

    public void rewriteTablePath(ConnectorSession session, String schema, String tableName, String sourcePrefix, String targetPrefix)
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

            // Serialize the current metadata to JSON, replace all path occurrences, then
            // deserialize back and write to the target location. This rewrites the table
            // location, snapshot manifest-list pointers, and any explicit location properties
            // in a single pass without manipulating the object model.
            String sourceJson = TableMetadataParser.toJson(sourceMetadata);
            String targetJson = sourceJson.replace(normalizedSource, normalizedTarget);

            String newMetadataLocation = sourceMetadata.metadataFileLocation()
                    .replace(normalizedSource, normalizedTarget);

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);
            OutputFile outputFile = fileIO.newOutputFile(newMetadataLocation);

            TableMetadata newMetadata = TableMetadataParser.fromJson(newMetadataLocation, targetJson);
            TableMetadataParser.write(newMetadata, outputFile);
        }
    }
}
