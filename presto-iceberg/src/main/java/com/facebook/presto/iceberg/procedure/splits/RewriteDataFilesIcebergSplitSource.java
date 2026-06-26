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
package com.facebook.presto.iceberg.procedure.splits;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import static com.facebook.presto.iceberg.IcebergUtil.UNLIMITED_CONCURRENCY;
import static com.facebook.presto.iceberg.IcebergUtil.filterAndGroupByPartition;
import static com.facebook.presto.iceberg.IcebergUtil.filterByFile;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static com.facebook.presto.iceberg.IcebergUtil.parseMaxConcurrentFileGroupRewrites;

/**
 * Split source for rewrite_data_files procedure that applies filtering
 * based on procedure options. Supports file-level filters
 * (min-file-size-bytes, max-file-size-bytes) and group-level filters
 * (min-input-files).
 * <p>
 * Optionally limits partition-level batching via the
 * max-concurrent-file-group-rewrites option. When enabled, uses the
 * already-grouped partition structure from filterAndGroupByPartition and
 * dynamically activates at most N partitions at a time, yielding tasks
 * from active partitions sequentially. This controls split generation
 * batching, not worker-level execution concurrency.
 */
public class RewriteDataFilesIcebergSplitSource
        extends IcebergSplitSource
{
    /**
     * Creates a split source for the rewrite_data_files procedure with
     * optional concurrency limiting.
     *
     * @param session the connector session
     * @param tableScan the table scan to generate splits from
     * @param metadataColumnConstraints metadata column constraints
     * @param options rewrite procedure options
     */
    public RewriteDataFilesIcebergSplitSource(
            final ConnectorSession session,
            final TableScan tableScan,
            final TupleDomain<IcebergColumnHandle> metadataColumnConstraints,
            final Map<String, String> options)
    {
        super(
                session,
                getTargetSplitSize(session, tableScan).toBytes(),
                applyFiltersAndConcurrencyLimit(tableScan, options),
                metadataColumnConstraints);
    }

    /**
     * Applies file-level filters, groups by partition, applies group-level
     * filters, then optionally wraps with concurrency limiting.
     *
     * @param tableScan the table scan to filter
     * @param options rewrite options map
     * @return filtered and optionally concurrency-limited tasks
     */
    private static CloseableIterable<FileScanTask>
            applyFiltersAndConcurrencyLimit(
                    final TableScan tableScan,
                    final Map<String, String> options)
    {
        int maxConcurrentFileGroups =
                parseMaxConcurrentFileGroupRewrites(options);

        // Apply file-level filters first, then group by partition with
        // partition-level filters
        Map<String, List<FileScanTask>> partitionGroups =
                filterAndGroupByPartition(
                        filterByFile(tableScan.planFiles(), options),
                        options);

        // If no concurrency limit, just flatten and return all tasks
        if (maxConcurrentFileGroups <= UNLIMITED_CONCURRENCY) {
            List<FileScanTask> allTasks = new ArrayList<>();
            for (List<FileScanTask> partitionTasks : partitionGroups.values()) {
                allTasks.addAll(partitionTasks);
            }
            return CloseableIterable.withNoopClose(allTasks);
        }

        // Otherwise, wrap with concurrency limiting iterator
        return new CloseableIterable<FileScanTask>()
        {
            @Override
            public CloseableIterator<FileScanTask> iterator()
            {
                return new PartitionLimitingIterator(
                        partitionGroups, maxConcurrentFileGroups);
            }

            @Override
            public void close()
            {
            }
        };
    }

    /**
     * Iterator that dynamically activates partitions sequentially.
     * Works with pre-grouped partition structure from
     * filterAndGroupByPartition. Maintains at most maxConcurrentPartitions
     * active partitions, activating new ones as old ones exhaust.
     * Processes partitions sequentially - exhausts one partition before
     * moving to the next.
     */
    private static class PartitionLimitingIterator
            implements CloseableIterator<FileScanTask>
    {
        /** Map of partition key to list of tasks for that partition. */
        private final Map<String, List<FileScanTask>> tasks;
        /** Maximum number of partitions to have active concurrently. */
        private final int maxConcurrent;
        /** Queue of partition keys waiting to be activated. */
        private final Queue<String> pendingPartitions = new LinkedList<>();
        /** List of currently active partition keys. */
        private final List<String> activePartitions = new ArrayList<>();

        /**
         * Creates a partition-limiting iterator.
         *
         * @param partitionedTasks map of partition key to tasks
         * @param maxConcurrentPartitions maximum concurrent partitions
         */
        PartitionLimitingIterator(
                final Map<String, List<FileScanTask>> partitionedTasks,
                final int maxConcurrentPartitions)
        {
            this.tasks = ImmutableMap.copyOf(partitionedTasks);
            this.maxConcurrent = maxConcurrentPartitions;

            // Initialize pending partitions with all partition keys
            pendingPartitions.addAll(tasks.keySet());

            // Activate initial set of partitions
            activateNextPartitions();
        }

        @Override
        public boolean hasNext()
        {
            // Clean up exhausted partitions and try to activate new ones
            cleanupAndActivate();

            // Check if any active partition has tasks
            for (String partition : activePartitions) {
                if (!tasks.get(partition).isEmpty()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public FileScanTask next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // Process the first active partition with tasks
            for (String partition : activePartitions) {
                List<FileScanTask> partitionTasks = tasks.get(partition);

                if (partitionTasks != null && !partitionTasks.isEmpty()) {
                    return partitionTasks.remove(0);
                }
            }

            throw new NoSuchElementException();
        }

        /**
         * Removes empty partitions from active set and activates new ones.
         */
        private void cleanupAndActivate()
        {
            activePartitions.removeIf(
                    partition -> tasks.get(partition).isEmpty());
            activateNextPartitions();
        }

        /**
         * Activates pending partitions up to the maximum concurrent limit.
         */
        private void activateNextPartitions()
        {
            while (activePartitions.size() < maxConcurrent
                    && !pendingPartitions.isEmpty()) {
                String nextPartition = pendingPartitions.poll();
                if (nextPartition != null
                        && !tasks.get(nextPartition).isEmpty()) {
                    activePartitions.add(nextPartition);
                }
            }
        }

        @Override
        public void close()
        {
        }
    }
}
