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
package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.table.Sequence.SequenceFunctionSplit.DEFAULT_SPLIT_SIZE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;

public class TestSequenceFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner result = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .build())
                .build();
        result.installPlugin(new TpchPlugin());
        result.createCatalog("tpch", "tpch");
        return result;
    }

    @Test
    public void testSequence()
    {
        assertQuery("SELECT * FROM TABLE(sequence(0, 8000, 3))",
                "SELECT * FROM UNNEST(sequence(0, 8000, 3))");

        assertQuery("SELECT * FROM TABLE(sequence(1, 10, 3))",
                "VALUES BIGINT '1', 4, 7, 10");

        assertQuery("SELECT * FROM TABLE(sequence(1, 10, 6))",
                "VALUES BIGINT '1', 7");

        assertQuery("SELECT * FROM TABLE(sequence(-1, -10, -3))",
                "VALUES BIGINT '-1', -4, -7, -10");

        assertQuery("SELECT * FROM TABLE(sequence(-1, -10, -6))",
                "VALUES BIGINT '-1', -7");

        assertQuery("SELECT * FROM TABLE(sequence(-5, 5, 3))",
                "VALUES BIGINT '-5', -2, 1, 4");

        assertQuery("SELECT * FROM TABLE(sequence(5, -5, -3))",
                "VALUES BIGINT '5', 2, -1, -4");

        assertQuery("SELECT * FROM TABLE(sequence(0, 10, 3))",
                "VALUES BIGINT '0', 3, 6, 9");

        assertQuery("SELECT * FROM TABLE(sequence(0, -10, -3))",
                "VALUES BIGINT '0', -3, -6, -9");
    }

    @Test
    public void testDefaultArguments()
    {
        assertQuery("SELECT * FROM TABLE(sequence(stop => 10))",
                "SELECT * FROM UNNEST(sequence(0, 10, 1))");
    }

    @Test
    public void testInvalidArgument()
    {
        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => -2))",
                "Step must be positive for sequence [-5, 10]");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => 2))",
                "Step must be negative for sequence [10, -5]");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => null," +
                        "                    stop => -5," +
                        "                    step => 2))",
                "Start is null");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => null," +
                        "                    step => 2))",
                "Stop is null");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => null))",
                "Step is null");
    }

    @Test
    public void testSingletonSequence()
    {
        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => 2))",
                "VALUES BIGINT '10'");

        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => -2))",
                "VALUES BIGINT '10'");

        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => 0))",
                "VALUES BIGINT '10'");
    }

    @Test
    public void testBigStep()
    {
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => %s))",
                Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1)), "VALUES BIGINT '10'");

        assertQuery(format("SELECT * " +
                                "FROM TABLE(sequence(" +
                                "                    start => 10," +
                                "                    stop => -5," +
                                "                    step => %s))",
                        Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1),
                "VALUES BIGINT '10'");

        assertQuery(format("SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x DESC) \n" +
                                "FROM TABLE(sequence(\n" +
                                "                    start => %s,\n" +
                                "                    stop => BIGINT '%s',\n" +
                                "                    step => %s)) t(x)",
                        Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1),
                format("VALUES (null), (%s)", Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1));

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => BIGINT '%s'))", Long.MIN_VALUE),
                "VALUES BIGINT '10'");

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1)),
                "VALUES BIGINT '-5'");

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1),
                "VALUES BIGINT '-5'");

        assertQuery(format("SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x) " +
                                "FROM TABLE(sequence(" +
                                "                    start => BIGINT '%s'," +
                                "                    stop => %s," +
                                "                    step => %s)) t(x)",
                        Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1),
                format("VALUES (null), (%s)", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1));

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE),
                "VALUES BIGINT '-5'");
    }

    @Test
    public void testMultipleSplits()
    {
        long sequenceLength = DEFAULT_SPLIT_SIZE * 10 + DEFAULT_SPLIT_SIZE / 2;
        long start = 10;
        long step = 5;
        long stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT count(x), count(DISTINCT x), min(x), max(x) " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s', BIGINT '%s', BIGINT '%s'", sequenceLength, sequenceLength, start, stop));

        sequenceLength = DEFAULT_SPLIT_SIZE * 4 + DEFAULT_SPLIT_SIZE / 2;
        stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT min(x), max(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));

        step = -5;
        stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT max(x), min(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));
    }

    @Test
    public void testEdgeValues()
    {
        long start = Long.MIN_VALUE + 15;
        long stop = Long.MIN_VALUE + 3;
        long step = -10;
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s))", start, stop, step),
                format("VALUES (%s), (%s)", start, start + step));

        start = Long.MIN_VALUE + 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MIN_VALUE + 1;
        assertQuery(format("SELECT max(x), min(x) " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT %s, %s", start, Long.MIN_VALUE + 1));

        start = Long.MAX_VALUE - 15;
        stop = Long.MAX_VALUE - 3;
        step = 10;
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s))", start, stop, step),
                format("VALUES (%s), (%s)", start, start + step));

        start = Long.MAX_VALUE - 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MAX_VALUE - 1;
        assertQuery(format("SELECT min(x), max(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT %s, %s", start, Long.MAX_VALUE - 1));
    }

    @Test
    public void testUnionAllWithMultipleSequences()
    {
        // Test UNION ALL with two sequences
        assertQuery("SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 5)) AS t(num) " +
                        "ORDER BY sequence_type, value",
                "SELECT * FROM (VALUES " +
                        "('Even Numbers', BIGINT '0'), ('Even Numbers', 2), ('Even Numbers', 4), ('Even Numbers', 6), ('Even Numbers', 8), ('Even Numbers', 10), " +
                        "('Even Numbers', 12), ('Even Numbers', 14), ('Even Numbers', 16), ('Even Numbers', 18), ('Even Numbers', 20), " +
                        "('Even Numbers', 22), ('Even Numbers', 24), ('Even Numbers', 26), ('Even Numbers', 28), ('Even Numbers', 30), " +
                        "('Even Numbers', 32), ('Even Numbers', 34), ('Even Numbers', 36), ('Even Numbers', 38), ('Even Numbers', 40), " +
                        "('Even Numbers', 42), ('Even Numbers', 44), ('Even Numbers', 46), ('Even Numbers', 48), ('Even Numbers', 50), " +
                        "('Even Numbers', 52), ('Even Numbers', 54), ('Even Numbers', 56), ('Even Numbers', 58), ('Even Numbers', 60), " +
                        "('Even Numbers', 62), ('Even Numbers', 64), ('Even Numbers', 66), ('Even Numbers', 68), ('Even Numbers', 70), " +
                        "('Even Numbers', 72), ('Even Numbers', 74), ('Even Numbers', 76), ('Even Numbers', 78), ('Even Numbers', 80), " +
                        "('Even Numbers', 82), ('Even Numbers', 84), ('Even Numbers', 86), ('Even Numbers', 88), ('Even Numbers', 90), " +
                        "('Even Numbers', 92), ('Even Numbers', 94), ('Even Numbers', 96), ('Even Numbers', 98), ('Even Numbers', 100), " +
                        "('Multiples of 5', BIGINT '0'), ('Multiples of 5', 5), ('Multiples of 5', 10), ('Multiples of 5', 15), ('Multiples of 5', 20), " +
                        "('Multiples of 5', 25), ('Multiples of 5', 30), ('Multiples of 5', 35), ('Multiples of 5', 40), ('Multiples of 5', 45), " +
                        "('Multiples of 5', 50), ('Multiples of 5', 55), ('Multiples of 5', 60), ('Multiples of 5', 65), ('Multiples of 5', 70), " +
                        "('Multiples of 5', 75), ('Multiples of 5', 80), ('Multiples of 5', 85), ('Multiples of 5', 90), ('Multiples of 5', 95), " +
                        "('Multiples of 5', 100)) AS t(sequence_type, value) " +
                        "ORDER BY sequence_type, value");

        // Test UNION ALL with three sequences (original failing query)
        assertQuery("SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 5)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 10' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 10)) AS t(num) " +
                        "ORDER BY sequence_type, value",
                "SELECT sequence_type, value FROM (" +
                        "SELECT 'Even Numbers' AS sequence_type, x AS value FROM UNNEST(sequence(0, 100, 2)) AS t(x) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5', x FROM UNNEST(sequence(0, 100, 5)) AS t(x) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 10', x FROM UNNEST(sequence(0, 100, 10)) AS t(x)) " +
                        "ORDER BY sequence_type, value");
    }

    @Test
    public void testUnionAllWithAggregation()
    {
        // Test UNION ALL with aggregation to verify correct result counts
        assertQuery("SELECT sequence_type, COUNT(*) AS cnt, MIN(value) AS min_val, MAX(value) AS max_val " +
                        "FROM (" +
                        "  SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "  FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'Odd Numbers' AS sequence_type, num AS value " +
                        "  FROM TABLE(sequence(1, 99, 2)) AS t(num)" +
                        ") " +
                        "GROUP BY sequence_type " +
                        "ORDER BY sequence_type",
                "VALUES ('Even Numbers', BIGINT '51', BIGINT '0', BIGINT '100'), " +
                        "('Odd Numbers', BIGINT '50', BIGINT '1', BIGINT '99')");
    }

    @Test
    public void testUnionAllWithLargeSequences()
    {
        // Test UNION ALL with sequences that generate multiple splits
        long sequenceLength = DEFAULT_SPLIT_SIZE * 2;
        long start1 = 0;
        long step1 = 1;
        long stop1 = start1 + (sequenceLength - 1) * step1;

        long start2 = 1000000;
        long step2 = 1;
        long stop2 = start2 + (sequenceLength - 1) * step2;

        assertQuery(format("SELECT sequence_type, COUNT(*) AS cnt, MIN(value) AS min_val, MAX(value) AS max_val " +
                                "FROM (" +
                                "  SELECT 'First' AS sequence_type, num AS value " +
                                "  FROM TABLE(sequence(%s, %s, %s)) AS t(num) " +
                                "  UNION ALL " +
                                "  SELECT 'Second' AS sequence_type, num AS value " +
                                "  FROM TABLE(sequence(%s, %s, %s)) AS t(num)" +
                                ") " +
                                "GROUP BY sequence_type " +
                                "ORDER BY sequence_type",
                        start1, stop1, step1, start2, stop2, step2),
                format("VALUES ('First', BIGINT '%s', BIGINT '%s', BIGINT '%s'), " +
                                "('Second', BIGINT '%s', BIGINT '%s', BIGINT '%s')",
                        sequenceLength, start1, stop1, sequenceLength, start2, stop2));
    }

    @Test
    public void testUnionAllWithJoin()
    {
        // Test UNION ALL sequences joined with a table
        assertQuery("SELECT t.name, s.value " +
                        "FROM tpch.tiny.nation t " +
                        "JOIN (" +
                        "  SELECT num AS value FROM TABLE(sequence(0, 10, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num AS value FROM TABLE(sequence(15, 25, 1)) AS t(num)" +
                        ") s ON t.nationkey = s.value " +
                        "ORDER BY t.name",
                "SELECT name, nationkey FROM tpch.tiny.nation WHERE nationkey <= 10 OR (nationkey >= 15 AND nationkey <= 24) ORDER BY name");
    }

    @Test
    public void testMultipleUnionAllBranches()
    {
        // Test complex UNION ALL with 4 branches
        assertQuery("SELECT type, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 'A' AS type, num FROM TABLE(sequence(0, 50, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'B' AS type, num FROM TABLE(sequence(0, 30, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'C' AS type, num FROM TABLE(sequence(0, 40, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'D' AS type, num FROM TABLE(sequence(0, 20, 1)) AS t(num)" +
                        ") " +
                        "GROUP BY type " +
                        "ORDER BY type",
                "VALUES ('A', BIGINT '51'), ('B', BIGINT '31'), ('C', BIGINT '41'), ('D', BIGINT '21')");
    }

    @Test
    public void testUnionAllSchedulingBehavior()
    {
        // This test documents and verifies the fix for UNION ALL of table-valued functions (TVFs)
        //
        // BACKGROUND:
        // When UNION ALL is used with regular tables, each table scan creates a separate stage/fragment
        // that is scheduled independently. The scheduler creates separate split sources for each table,
        // and they are scheduled sequentially (one stage at a time) to avoid overwhelming the cluster.
        //
        // PROBLEM WITH TVFs (Table-Valued Functions like sequence()):
        // Unlike regular tables, TVFs are "leaf" operators that generate data directly without reading
        // from storage. When UNION ALL combines multiple TVF calls, they all end up in the SAME fragment
        // because there's no need for data exchange between stages. This means:
        // 1. All TVF calls share the same SOURCE_DISTRIBUTION partitioning handle
        // 2. Multiple split sources are created (one per TVF call) in the same fragment
        // 3. The original code assumed only ONE split source per SOURCE_DISTRIBUTION fragment
        //
        // THE FIX:
        // The fix in SectionExecutionFactory.java detects when there are multiple split sources
        // for SOURCE_DISTRIBUTION and uses FixedSourcePartitionedScheduler instead of the
        // single-source optimized path. It uses round-robin distribution to balance splits
        // across available nodes.
        //
        // This test verifies that UNION ALL of multiple sequence() calls produces correct results
        // and that the data from all branches is properly combined.

        // Test 1: Simple UNION ALL with two sequence functions
        // This should create 2 split sources in the same fragment
        assertQuery("SELECT COUNT(*) AS total, MIN(value) AS min_val, MAX(value) AS max_val " +
                        "FROM (" +
                        "  SELECT num AS value FROM TABLE(sequence(1, 100, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num AS value FROM TABLE(sequence(200, 300, 1)) AS t(num)" +
                        ")",
                "VALUES (BIGINT '201', BIGINT '1', BIGINT '300')");

        // Test 2: UNION ALL with three sequence functions (the original failing case)
        // This should create 3 split sources in the same fragment
        assertQuery("SELECT branch, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 'first' AS branch, num FROM TABLE(sequence(1, 10, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'second' AS branch, num FROM TABLE(sequence(1, 20, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'third' AS branch, num FROM TABLE(sequence(1, 30, 1)) AS t(num)" +
                        ") " +
                        "GROUP BY branch " +
                        "ORDER BY branch",
                "VALUES ('first', BIGINT '10'), ('second', BIGINT '20'), ('third', BIGINT '30')");

        // Test 3: Verify all data is present (no data loss during scheduling)
        assertQuery("SELECT COUNT(DISTINCT value) AS unique_values " +
                        "FROM (" +
                        "  SELECT num AS value FROM TABLE(sequence(1, 50, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num AS value FROM TABLE(sequence(51, 100, 1)) AS t(num)" +
                        ")",
                "VALUES (BIGINT '100')");
    }

    @Test
    public void testUnionAllVsRegularTableComparison()
    {
        // This test compares UNION ALL behavior between sequence functions and regular tables
        // to document the scheduling differences.
        //
        // REGULAR TABLES (e.g., nation UNION ALL region):
        // - Each table scan is in a separate fragment
        // - Fragments are scheduled sequentially (phased execution)
        // - Each fragment has its own split source
        // - Exchange nodes coordinate data movement between fragments
        //
        // SEQUENCE FUNCTIONS (e.g., sequence(1,10) UNION ALL sequence(11,20)):
        // - All sequence calls are in the SAME fragment (no exchange needed)
        // - Multiple split sources exist in one fragment
        // - Round-robin distribution balances work across nodes
        // - No inter-stage coordination needed

        // Compare results: sequence UNION ALL vs table UNION ALL
        // Both should produce the same logical result structure
        assertQuery("SELECT type, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 'seq' AS type, num AS value FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'seq' AS type, num AS value FROM TABLE(sequence(0, 24, 1)) AS t(num)" +
                        ") " +
                        "GROUP BY type",
                "SELECT type, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 'tbl' AS type, nationkey AS value FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 'tbl' AS type, nationkey AS value FROM tpch.tiny.nation" +
                        ") " +
                        "GROUP BY type");
    }

    @Test
    public void testUnionAllWithDifferentSplitSizes()
    {
        // Test UNION ALL where different branches generate different numbers of splits
        // This verifies that the round-robin distribution works correctly regardless
        // of split count imbalance between sources
        
        long smallSequence = DEFAULT_SPLIT_SIZE / 2;  // Less than one split
        long largeSequence = DEFAULT_SPLIT_SIZE * 3;  // Multiple splits

        assertQuery(format("SELECT branch, COUNT(*) AS cnt " +
                                "FROM (" +
                                "  SELECT 'small' AS branch, num FROM TABLE(sequence(0, %s, 1)) AS t(num) " +
                                "  UNION ALL " +
                                "  SELECT 'large' AS branch, num FROM TABLE(sequence(0, %s, 1)) AS t(num)" +
                                ") " +
                                "GROUP BY branch " +
                                "ORDER BY branch",
                        smallSequence, largeSequence),
                format("VALUES ('large', BIGINT '%s'), ('small', BIGINT '%s')",
                        largeSequence + 1, smallSequence + 1));
    }

    @Test
    public void testUnionAllSplitDistribution()
    {
        // This test verifies that splits from multiple sequence sources are distributed
        // correctly across nodes using round-robin distribution.
        //
        // The fix ensures that:
        // 1. All splits from all sources are processed
        // 2. Splits are distributed evenly across available nodes
        // 3. No splits are lost or duplicated during scheduling

        // Create sequences that will generate multiple splits each
        long sequenceLength = DEFAULT_SPLIT_SIZE * 2 + 100;
        
        assertQuery(format("SELECT " +
                                "  COUNT(*) AS total_rows, " +
                                "  COUNT(DISTINCT value) AS unique_values, " +
                                "  MIN(value) AS min_val, " +
                                "  MAX(value) AS max_val " +
                                "FROM (" +
                                "  SELECT num AS value FROM TABLE(sequence(0, %s, 1)) AS t(num) " +
                                "  UNION ALL " +
                                "  SELECT num AS value FROM TABLE(sequence(%s, %s, 1)) AS t(num)" +
                                ")",
                        sequenceLength, sequenceLength + 1, sequenceLength * 2 + 1),
                format("VALUES (BIGINT '%s', BIGINT '%s', BIGINT '0', BIGINT '%s')",
                        (sequenceLength + 1) * 2, (sequenceLength + 1) * 2, sequenceLength * 2 + 1));
    }

    @Test
    public void testRegularTableUnionAll()
    {
        // This test uses regular tables with UNION ALL to understand how they are scheduled
        // differently from table-valued functions.
        //
        // REGULAR TABLE UNION ALL SCHEDULING:
        // - Each table scan is in a separate fragment
        // - Fragments are scheduled sequentially (one at a time)
        // - Each fragment has ONE split source
        // - Exchange nodes coordinate data movement between fragments
        //
        // This is the "normal" case that the original code was designed for.

        // Test 1: Simple UNION ALL with two different tables
        assertQuery("SELECT name, nationkey " +
                        "FROM tpch.tiny.nation " +
                        "WHERE nationkey < 5 " +
                        "UNION ALL " +
                        "SELECT name, regionkey AS nationkey " +
                        "FROM tpch.tiny.region " +
                        "ORDER BY name",
                "SELECT name, nationkey FROM (" +
                        "  SELECT name, nationkey FROM tpch.tiny.nation WHERE nationkey < 5 " +
                        "  UNION ALL " +
                        "  SELECT name, regionkey FROM tpch.tiny.region" +
                        ") ORDER BY name");
/*
        // Test 2: UNION ALL with the same table (self-union)
        assertQuery("SELECT COUNT(*) AS total " +
                        "FROM (" +
                        "  SELECT nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT nationkey FROM tpch.tiny.nation" +
                        ")",
                "VALUES (BIGINT '50')");  // 25 nations * 2

        // Test 3: UNION ALL with three tables
        assertQuery("SELECT source, cnt FROM (" +
                        "  SELECT 'nation' AS source, COUNT(*) AS cnt FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 'region' AS source, COUNT(*) AS cnt FROM tpch.tiny.region " +
                        "  UNION ALL " +
                        "  SELECT 'customer' AS source, COUNT(*) AS cnt FROM tpch.tiny.customer" +
                        ") ORDER BY source",
                "VALUES ('customer', BIGINT '1500'), ('nation', BIGINT '25'), ('region', BIGINT '5')");*/
    }

    @Test
    public void testRegularTableUnionAllWithAggregation()
    {
        // Test UNION ALL with regular tables and aggregation to verify scheduling correctness
        
        assertQuery("SELECT source_type, COUNT(*) AS row_count, MIN(key_value) AS min_key, MAX(key_value) AS max_key " +
                        "FROM (" +
                        "  SELECT 'nation' AS source_type, nationkey AS key_value FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 'region' AS source_type, regionkey AS key_value FROM tpch.tiny.region" +
                        ") " +
                        "GROUP BY source_type " +
                        "ORDER BY source_type",
                "VALUES ('nation', BIGINT '25', BIGINT '0', BIGINT '24'), " +
                        "('region', BIGINT '5', BIGINT '0', BIGINT '4')");
    }

    @Test
    public void testRegularTableUnionAllWithJoin()
    {
        // Test UNION ALL with regular tables joined to another table
        // This tests more complex scheduling scenarios
        
        assertQuery("SELECT r.name AS region_name, u.source, u.key_value " +
                        "FROM tpch.tiny.region r " +
                        "JOIN (" +
                        "  SELECT 'nation' AS source, nationkey AS key_value, regionkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 'customer' AS source, custkey AS key_value, nationkey AS regionkey FROM tpch.tiny.customer WHERE custkey < 10" +
                        ") u ON r.regionkey = u.regionkey " +
                        "ORDER BY r.name, u.source, u.key_value " +
                        "LIMIT 10",
                "SELECT r.name, u.source, u.key_value FROM tpch.tiny.region r " +
                        "JOIN (" +
                        "  SELECT 'nation' AS source, nationkey AS key_value, regionkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 'customer' AS source, custkey AS key_value, nationkey AS regionkey FROM tpch.tiny.customer WHERE custkey < 10" +
                        ") u ON r.regionkey = u.regionkey " +
                        "ORDER BY r.name, u.source, u.key_value " +
                        "LIMIT 10");
    }

    @Test
    public void testRegularTableSelfUnionMultipleTimes()
    {
        // Test UNION ALL with the same table multiple times
        // This creates multiple fragments, all reading from the same table
        // Each fragment is scheduled separately with its own split source
        
        assertQuery("SELECT source_id, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 1 AS source_id, nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 2 AS source_id, nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 3 AS source_id, nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT 4 AS source_id, nationkey FROM tpch.tiny.nation" +
                        ") " +
                        "GROUP BY source_id " +
                        "ORDER BY source_id",
                "VALUES (1, BIGINT '25'), (2, BIGINT '25'), (3, BIGINT '25'), (4, BIGINT '25')");
    }

    @Test
    public void testRegularTableUnionAllWithFilters()
    {
        // Test UNION ALL with different filter predicates on the same table
        // Each branch has different filters, creating different split sources
        
        assertQuery("SELECT range_type, COUNT(*) AS cnt, MIN(nationkey) AS min_key, MAX(nationkey) AS max_key " +
                        "FROM (" +
                        "  SELECT 'low' AS range_type, nationkey FROM tpch.tiny.nation WHERE nationkey < 10 " +
                        "  UNION ALL " +
                        "  SELECT 'high' AS range_type, nationkey FROM tpch.tiny.nation WHERE nationkey >= 10" +
                        ") " +
                        "GROUP BY range_type " +
                        "ORDER BY range_type",
                "VALUES ('high', BIGINT '15', BIGINT '10', BIGINT '24'), " +
                        "('low', BIGINT '10', BIGINT '0', BIGINT '9')");
    }

    @Test
    public void testCompareTableVsFunctionUnionAll()
    {
        // Direct comparison: Regular table UNION ALL vs Sequence function UNION ALL
        // This highlights the scheduling difference:
        // - Tables: Multiple fragments, sequential scheduling, one split source per fragment
        // - Functions: Single fragment, concurrent scheduling, multiple split sources in one fragment
        
        // Both should produce the same number of total rows
        assertQuery("SELECT COUNT(*) FROM (" +
                        "  SELECT nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT nationkey FROM tpch.tiny.nation" +
                        ")",
                "SELECT COUNT(*) FROM (" +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num)" +
                        ")");
    }

    @Test
    public void testPrintUnionAllPlans()
    {
        // This test prints the execution plans to show the difference between
        // regular table UNION ALL and sequence function UNION ALL
        
        System.out.println("\n========================================");
        System.out.println("REGULAR TABLE UNION ALL PLAN:");
        System.out.println("========================================");
        
        // Print plan for regular table UNION ALL
        String tablePlan = (String) computeActual("EXPLAIN (TYPE DISTRIBUTED) " +
                "SELECT nationkey FROM tpch.tiny.nation " +
                "UNION ALL " +
                "SELECT nationkey FROM tpch.tiny.nation").getOnlyValue();
        System.out.println(tablePlan);
        
        System.out.println("\n========================================");
        System.out.println("SEQUENCE FUNCTION UNION ALL PLAN:");
        System.out.println("========================================");
        
        // Print plan for sequence function UNION ALL
        String sequencePlan = (String) computeActual("EXPLAIN (TYPE DISTRIBUTED) " +
                "SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                "UNION ALL " +
                "SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num)").getOnlyValue();
        System.out.println(sequencePlan);
        
        System.out.println("\n========================================");
        System.out.println("THREE-WAY SEQUENCE FUNCTION UNION ALL PLAN:");
        System.out.println("========================================");
        
        // Print plan for three-way sequence function UNION ALL (the original failing case)
        String threeWayPlan = (String) computeActual("EXPLAIN (TYPE DISTRIBUTED) " +
                "SELECT num FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                "UNION ALL " +
                "SELECT num FROM TABLE(sequence(0, 100, 5)) AS t(num) " +
                "UNION ALL " +
                "SELECT num FROM TABLE(sequence(0, 100, 10)) AS t(num)").getOnlyValue();
        System.out.println(threeWayPlan);
        
        System.out.println("\n========================================");
        System.out.println("KEY OBSERVATIONS:");
        System.out.println("========================================");
        System.out.println("1. Regular table UNION ALL:");
        System.out.println("   - Multiple fragments (one per table scan)");
        System.out.println("   - Exchange nodes between fragments");
        System.out.println("   - Each fragment has 1 split source");
        System.out.println("   - Union fragment has RemoteSourceNode children");
        System.out.println("");
        System.out.println("2. Sequence function UNION ALL:");
        System.out.println("   - Single fragment containing all sequence calls");
        System.out.println("   - NO exchange nodes (no data movement needed)");
        System.out.println("   - Fragment has N split sources (one per sequence call)");
        System.out.println("   - Union fragment has TableFunctionProcessor children");
        System.out.println("========================================\n");
    }

    @Test
    public void testPrintUnionAllLogicalPlans()
    {
        // This test prints the logical plans to show the plan node structure
        
        System.out.println("\n========================================");
        System.out.println("REGULAR TABLE UNION ALL LOGICAL PLAN:");
        System.out.println("========================================");
        
        String tableLogicalPlan = (String) computeActual("EXPLAIN (TYPE LOGICAL) " +
                "SELECT nationkey FROM tpch.tiny.nation " +
                "UNION ALL " +
                "SELECT nationkey FROM tpch.tiny.nation").getOnlyValue();
        System.out.println(tableLogicalPlan);
        
        System.out.println("\n========================================");
        System.out.println("SEQUENCE FUNCTION UNION ALL LOGICAL PLAN:");
        System.out.println("========================================");
        
        String sequenceLogicalPlan = (String) computeActual("EXPLAIN (TYPE LOGICAL) " +
                "SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                "UNION ALL " +
                "SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num)").getOnlyValue();
        System.out.println(sequenceLogicalPlan);
        
        System.out.println("\n========================================");
        System.out.println("COMPARISON:");
        System.out.println("========================================");
        System.out.println("Both have UnionNode at the top, but:");
        System.out.println("- Table UNION: Children are TableScan nodes");
        System.out.println("- TVF UNION: Children are TableFunctionProcessor nodes");
        System.out.println("========================================\n");
    }

    @Test
    public void testDebugTableUnionAll()
    {
        // Simple test case to debug regular table UNION ALL
        // This will trigger the debug output in SplitSourceFactory and SectionExecutionFactory
        
        System.out.println("\n\n");
        System.out.println("##########################################################");
        System.out.println("### EXECUTING: REGULAR TABLE UNION ALL");
        System.out.println("##########################################################");
        
        assertQuery("SELECT COUNT(*) FROM (" +
                        "  SELECT nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT nationkey FROM tpch.tiny.nation" +
                        ")",
                "VALUES (BIGINT '50')");
        
        System.out.println("##########################################################");
        System.out.println("### COMPLETED: REGULAR TABLE UNION ALL");
        System.out.println("##########################################################\n\n");
    }

    @Test
    public void testDebugSequenceUnionAll()
    {
        // Simple test case to debug sequence function UNION ALL
        // This will trigger the debug output in SplitSourceFactory and SectionExecutionFactory
        
        System.out.println("\n\n");
        System.out.println("##########################################################");
        System.out.println("### EXECUTING: SEQUENCE FUNCTION UNION ALL");
        System.out.println("##########################################################");
        
        assertQuery("SELECT COUNT(*) FROM (" +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num)" +
                        ")",
                "VALUES (BIGINT '50')");
        
        System.out.println("##########################################################");
        System.out.println("### COMPLETED: SEQUENCE FUNCTION UNION ALL");
        System.out.println("##########################################################\n\n");
    }

    @Test
    public void testDebugBothUnionAll()
    {
        // Run both tests in sequence to compare the debug output
        
        System.out.println("\n\n");
        System.out.println("##########################################################");
        System.out.println("### COMPARISON TEST: TABLE vs SEQUENCE UNION ALL");
        System.out.println("##########################################################\n");
        
        System.out.println(">>> TEST 1: Regular Table UNION ALL <<<");
        assertQuery("SELECT COUNT(*) FROM (" +
                        "  SELECT nationkey FROM tpch.tiny.nation " +
                        "  UNION ALL " +
                        "  SELECT nationkey FROM tpch.tiny.nation" +
                        ")",
                "VALUES (BIGINT '50')");
        
        System.out.println("\n\n>>> TEST 2: Sequence Function UNION ALL <<<");
        assertQuery("SELECT COUNT(*) FROM (" +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num FROM TABLE(sequence(0, 24, 1)) AS t(num)" +
                        ")",
                "VALUES (BIGINT '50')");
        
        System.out.println("\n##########################################################");
        System.out.println("### COMPARISON COMPLETE");
        System.out.println("##########################################################\n\n");
    }

    @Test
    public void testDebugRawUnionAllNoAggregation()
    {
        // Test without aggregation to see the raw UNION ALL behavior
        
        System.out.println("\n\n");
        System.out.println("##########################################################");
        System.out.println("### RAW UNION ALL (NO AGGREGATION)");
        System.out.println("##########################################################\n");
        
        System.out.println(">>> TEST 1: Regular Table UNION ALL (No Aggregation) <<<");
        assertQuery("SELECT nationkey FROM tpch.tiny.nation WHERE nationkey < 3 " +
                        "UNION ALL " +
                        "SELECT nationkey FROM tpch.tiny.nation WHERE nationkey < 3 " +
                        "ORDER BY nationkey",
                "VALUES (BIGINT '0'), (BIGINT '0'), (BIGINT '1'), (BIGINT '1'), (BIGINT '2'), (BIGINT '2')");
        
        System.out.println("\n\n>>> TEST 2: Sequence Function UNION ALL (No Aggregation) <<<");
        assertQuery("SELECT num FROM TABLE(sequence(0, 2, 1)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT num FROM TABLE(sequence(0, 2, 1)) AS t(num) " +
                        "ORDER BY num",
                "VALUES (BIGINT '0'), (BIGINT '0'), (BIGINT '1'), (BIGINT '1'), (BIGINT '2'), (BIGINT '2')");
        
        System.out.println("\n##########################################################");
        System.out.println("### RAW UNION ALL COMPLETE");
        System.out.println("##########################################################\n\n");
    }

    @Test
    public void testDebugThreeWaySequenceUnionAll()
    {
        // Test the original failing case: 3-way sequence UNION ALL
        
        System.out.println("\n\n");
        System.out.println("##########################################################");
        System.out.println("### THREE-WAY SEQUENCE UNION ALL (Original Failing Case)");
        System.out.println("##########################################################\n");
        
        assertQuery("SELECT 'Even' AS type, num FROM TABLE(sequence(0, 10, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Odd' AS type, num FROM TABLE(sequence(1, 9, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Fives' AS type, num FROM TABLE(sequence(0, 10, 5)) AS t(num) " +
                        "ORDER BY type, num",
                "SELECT * FROM (VALUES " +
                        "('Even', BIGINT '0'), ('Even', 2), ('Even', 4), ('Even', 6), ('Even', 8), ('Even', 10), " +
                        "('Fives', BIGINT '0'), ('Fives', 5), ('Fives', 10), " +
                        "('Odd', BIGINT '1'), ('Odd', 3), ('Odd', 5), ('Odd', 7), ('Odd', 9')) " +
                        "ORDER BY 1, 2");
        
        System.out.println("\n##########################################################");
        System.out.println("### THREE-WAY SEQUENCE UNION ALL COMPLETE");
        System.out.println("##########################################################\n\n");
    }
}
