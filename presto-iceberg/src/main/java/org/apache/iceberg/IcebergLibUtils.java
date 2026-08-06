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
package org.apache.iceberg;

import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IcebergLibUtils
{
    private IcebergLibUtils()
    {}

    /**
     * Open a writer for a manifest list through Iceberg lib's package-private
     * {@code ManifestLists}, so a manifest list can be written with entries the caller has
     * adjusted. The returned appender is Iceberg's own format-version-specific writer.
     */
    public static FileAppender<ManifestFile> writeManifestList(
            int formatVersion,
            OutputFile manifestListFile,
            long snapshotId,
            Long parentSnapshotId,
            long sequenceNumber,
            Long firstRowId)
    {
        requireNonNull(manifestListFile, "manifestListFile is null");
        return ManifestLists.write(formatVersion, manifestListFile, snapshotId, parentSnapshotId, sequenceNumber, firstRowId);
    }

    /**
     * Call the method in Iceberg lib's protected class to set explicitly
     * whether to use incremental cleanup when expiring snapshots
     */
    public static ExpireSnapshots withIncrementalCleanup(ExpireSnapshots expireSnapshots, boolean incrementalCleanup)
    {
        requireNonNull(expireSnapshots, "expireSnapshots is null");
        checkArgument(expireSnapshots instanceof RemoveSnapshots, "expireSnapshots is not an instance of RemoveSnapshots");
        return ((RemoveSnapshots) expireSnapshots).withIncrementalCleanup(incrementalCleanup);
    }

    public static TableScanContext getScanContext(DataTableScan tableScan)
    {
        return tableScan.context();
    }
}
