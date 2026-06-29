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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.procedure.splits.RewriteDataFilesIcebergSplitSource;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for RewriteDataFilesIcebergSplitSource concurrency limiting behavior.
 * <p>
 * Note: These tests verify split-level batching behavior (which partitions have splits available
 * at any given time), not the actual worker-level task execution concurrency. The concurrency
 * limiting primarily affects task scheduling at the worker level, but this is validated through
 * observing split batch composition.
 */
public class TestRewriteDataFilesIcebergSplitSource
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    @Test
    public void testConcurrencyLimitRespectsMaxValueInFirstBatch()
            throws Exception
    {
        String tableName = "test_split_source_concurrency";
        try {
            // Create a partitioned table with 5 partitions, MANY files each
            // This ensures that even with a large batch request, we can observe the limit
            assertUpdate("CREATE TABLE " + tableName + " (id integer, partition_key varchar) WITH (partitioning = ARRAY['partition_key'])");
            for (int p = 1; p <= 5; p++) {
                // Insert 10 rows per partition across 10 separate INSERTs to create 10 files per partition
                for (int f = 1; f <= 10; f++) {
                    assertUpdate(format("INSERT INTO %s VALUES (%d, 'p%d')", tableName, p * 100 + f, p), 1);
                }
            }

            Table table = loadTable(tableName);
            TableScan scan = table.newScan();

            // Create split source with max-concurrent-file-group-rewrites=2
            Map<String, String> options = ImmutableMap.of(
                    "max-concurrent-file-group-rewrites", "2",
                    "rewrite-all", "true");

            ConnectorSplitSource splitSource = createSplitSource(scan, options);

            // Request ONLY 3 splits in first batch - this should not be enough to drain 2 partitions
            // With 10 files per partition and max=2, first batch should only see 2 partitions
            CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> futureBatch =
                    splitSource.getNextBatch(null, 3);
            ConnectorSplitSource.ConnectorSplitBatch batch = futureBatch.get();

            Set<String> firstBatchPartitions = new HashSet<>();
            for (ConnectorSplit split : batch.getSplits()) {
                if (split instanceof IcebergSplit) {
                    String partitionKey = ((IcebergSplit) split).getPartitionDataJson().orElse("__UNPARTITIONED__");
                    firstBatchPartitions.add(partitionKey);
                }
            }

            // With max=2, first batch of 3 splits should come from at most 2 partitions
            assertTrue(firstBatchPartitions.size() <= 2,
                    format("First batch should have at most 2 partitions with max=2, but found %d: %s",
                            firstBatchPartitions.size(), firstBatchPartitions));

            // Verify we eventually get all 5 partitions
            Set<String> allPartitions = new HashSet<>(firstBatchPartitions);
            while (!splitSource.isFinished()) {
                futureBatch = splitSource.getNextBatch(null, 10);
                batch = futureBatch.get();
                for (ConnectorSplit split : batch.getSplits()) {
                    if (split instanceof IcebergSplit) {
                        String partitionKey = ((IcebergSplit) split).getPartitionDataJson().orElse("__UNPARTITIONED__");
                        allPartitions.add(partitionKey);
                    }
                }
            }
            assertEquals(allPartitions.size(), 5, "Should eventually process all 5 partitions");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMaxConcurrentLimitActivatesPartitionsIncrementally()
            throws Exception
    {
        String tableName = "test_max_limit_incremental";
        try {
            // Create table with 6 partitions, each with MANY files
            assertUpdate("CREATE TABLE " + tableName + " (id integer, partition_key varchar) WITH (partitioning = ARRAY['partition_key'])");
            for (int p = 1; p <= 6; p++) {
                for (int f = 1; f <= 8; f++) {
                    assertUpdate(format("INSERT INTO %s VALUES (%d, 'p%d')", tableName, p * 100 + f, p), 1);
                }
            }

            Table table = loadTable(tableName);
            TableScan scan = table.newScan();

            // Set max=3 - should only activate 3 partitions initially
            Map<String, String> options = ImmutableMap.of(
                    "max-concurrent-file-group-rewrites", "3",
                    "rewrite-all", "true");

            ConnectorSplitSource splitSource = createSplitSource(scan, options);

            // Fetch just 4 splits - should come from at most 3 partitions (with 8 files per partition)
            CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> futureBatch =
                    splitSource.getNextBatch(null, 4);
            ConnectorSplitSource.ConnectorSplitBatch batch = futureBatch.get();

            Set<String> initialPartitions = new HashSet<>();
            for (ConnectorSplit split : batch.getSplits()) {
                if (split instanceof IcebergSplit) {
                    String partitionKey = ((IcebergSplit) split).getPartitionDataJson().orElse("__UNPARTITIONED__");
                    initialPartitions.add(partitionKey);
                }
            }

            // Should see at most 3 partitions in first batch
            assertTrue(initialPartitions.size() <= 3,
                    format("First batch should have at most 3 partitions with max=3, but found %d: %s",
                            initialPartitions.size(), initialPartitions));

            // Continue fetching - should eventually see all 6 partitions
            Set<String> allPartitions = new HashSet<>(initialPartitions);
            while (!splitSource.isFinished()) {
                futureBatch = splitSource.getNextBatch(null, 10);
                batch = futureBatch.get();
                for (ConnectorSplit split : batch.getSplits()) {
                    if (split instanceof IcebergSplit) {
                        String partitionKey = ((IcebergSplit) split).getPartitionDataJson().orElse("__UNPARTITIONED__");
                        allPartitions.add(partitionKey);
                    }
                }
            }
            assertEquals(allPartitions.size(), 6, "Should eventually process all 6 partitions");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testUnlimitedConcurrencyActivatesAllPartitions()
            throws Exception
    {
        String tableName = "test_unlimited_concurrency";
        try {
            // Create table with 5 partitions
            assertUpdate("CREATE TABLE " + tableName + " (id integer, partition_key varchar) WITH (partitioning = ARRAY['partition_key'])");
            for (int p = 1; p <= 5; p++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, 'p%d')", tableName, p, p), 1);
            }

            Table table = loadTable(tableName);
            TableScan scan = table.newScan();

            // No max-concurrent-file-group-rewrites option (unlimited)
            Map<String, String> options = ImmutableMap.of("rewrite-all", "true");

            ConnectorSplitSource splitSource = createSplitSource(scan, options);

            // With unlimited concurrency, all 5 partitions should be available immediately
            Set<String> allPartitions = new HashSet<>();
            CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> futureBatch =
                    splitSource.getNextBatch(null, 100);
            ConnectorSplitSource.ConnectorSplitBatch batch = futureBatch.get();

            for (ConnectorSplit split : batch.getSplits()) {
                if (split instanceof IcebergSplit) {
                    String partitionKey = ((IcebergSplit) split).getPartitionDataJson().orElse("__UNPARTITIONED__");
                    allPartitions.add(partitionKey);
                }
            }

            // All 5 partitions should appear in first batch (or very early batches)
            assertEquals(allPartitions.size(), 5, "All 5 partitions should be active immediately with unlimited concurrency");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private ConnectorSplitSource createSplitSource(TableScan scan, Map<String, String> options)
    {
        Session session = getQueryRunner().getDefaultSession();
        RewriteDataFilesIcebergSplitSource splitSource =
                new RewriteDataFilesIcebergSplitSource(
                        session.toConnectorSession(new ConnectorId(ICEBERG_CATALOG)),
                        scan,
                        com.facebook.presto.common.predicate.TupleDomain.all(),
                        options);
        return splitSource;
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }
}
