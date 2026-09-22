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
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;

/**
 * Reproduces the "statement is too large (stack overflow during analysis)" error
 * that occurs when querying a heavily partitioned Iceberg table with pushdown_filter_enabled=true.
 *
 * The table has ~3000 daily date partitions (created_date). When pushdown is enabled,
 * the planner builds a TupleDomain covering all 3000 partition values. Somewhere in the
 * optimizer pipeline this domain gets converted into a deeply left-nested expression tree
 * that overflows the JVM stack during recursive traversal.
 *
 * Run this test to find the exact optimizer rule that triggers the overflow by watching
 * the logged plan and optimizer step before the failure.
 */
public class TestLargePartitionStackOverflow
        extends AbstractTestQueryFramework
{
    private static final int PARTITION_COUNT = 3_000;
    private static final String TABLE = "large_partitioned_overflow";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"))
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    @BeforeClass
    public void createTable()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS tpch");
        queryRunner.execute(
                "CREATE TABLE " + TABLE + " (val INTEGER, created_date DATE) " +
                "WITH (partitioning = ARRAY['created_date'])");

        // Insert PARTITION_COUNT rows in batches of 90 to stay under the 100-open-writers limit.
        // Epoch-day 18000 = 2019-04-14.
        int batchSize = 90;
        for (int start = 18000; start < 18000 + PARTITION_COUNT; start += batchSize) {
            int end = Math.min(start + batchSize - 1, 18000 + PARTITION_COUNT - 1);
            queryRunner.execute(
                    "INSERT INTO " + TABLE + " SELECT 1, CAST(from_unixtime(CAST(d AS BIGINT) * 86400) AS DATE) " +
                    "FROM UNNEST(SEQUENCE(" + start + ", " + end + ")) AS t(d)");
        }
    }

    @AfterClass(alwaysRun = true)
    public void dropTable()
    {
        getQueryRunner().execute("DROP TABLE IF EXISTS " + TABLE);
    }

    /**
     * With pushdown_filter_enabled=true (the default on the cluster that hit this bug),
     * PLANNING a SELECT * on a table with 3000 partitions should trigger the stack overflow.
     * We use plan() rather than execute() to isolate the planning phase — the Java connector
     * rejects pushdown at execution time with NOT_SUPPORTED, so we never reach execution.
     *
     * If this test PASSES (no exception), the bug has been fixed or partition count is too low.
     * If it throws "statement is too large", the bug is still present.
     */
    @Test
    public void testSelectStarPlanFailsWithPushdownEnabled()
    {
        Session pushdownOn = pushdownSession(true);
        try {
            plan("SELECT * FROM " + TABLE + " LIMIT 1", pushdownOn);
            System.out.println("[INFO] SELECT * plan with pushdown=true completed without overflow " +
                    "(either fixed or PARTITION_COUNT=" + PARTITION_COUNT + " is below the threshold)");
        }
        catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("statement is too large")) {
                System.out.println("[CONFIRMED BUG] SELECT * plan with pushdown=true overflowed: " + e.getMessage());
                throw new AssertionError("Stack overflow reproduced with pushdown=true on " + PARTITION_COUNT + " partitions", e);
            }
            throw e;
        }
    }

    /**
     * Workaround: planning the same query with pushdown_filter_enabled=false should succeed.
     */
    @Test
    public void testSelectStarPlanSucceedsWithPushdownDisabled()
    {
        Session pushdownOff = pushdownSession(false);
        plan("SELECT * FROM " + TABLE + " LIMIT 1", pushdownOff);
    }

    /**
     * Same overflow check for COUNT(*).
     */
    @Test
    public void testCountStarPlanFailsWithPushdownEnabled()
    {
        Session pushdownOn = pushdownSession(true);
        try {
            plan("SELECT COUNT(*) FROM " + TABLE, pushdownOn);
            System.out.println("[INFO] SELECT COUNT(*) plan with pushdown=true completed without overflow");
        }
        catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("statement is too large")) {
                System.out.println("[CONFIRMED BUG] SELECT COUNT(*) plan with pushdown=true overflowed: " + e.getMessage());
                throw new AssertionError("Stack overflow reproduced for COUNT(*) with pushdown=true on " + PARTITION_COUNT + " partitions", e);
            }
            throw e;
        }
    }

    /**
     * Dump the logical plan with pushdown=true so we can inspect which optimizer step
     * last ran successfully before the overflow.
     */
    @Test
    public void testDumpPlanWithPushdownEnabled()
    {
        Session pushdownOn = pushdownSession(true);
        try {
            Plan plan = plan("SELECT COUNT(*) FROM " + TABLE, pushdownOn);
            System.out.println("[PLAN - pushdown=true]\n" +
                    textLogicalPlan(plan.getRoot(), plan.getTypes(), StatsAndCosts.empty(),
                            getQueryRunner().getMetadata().getFunctionAndTypeManager(), pushdownOn, 0));
        }
        catch (Exception e) {
            System.out.println("[PLAN DUMP FAILED with pushdown=true]: " + e.getMessage());
        }
    }

    /**
     * Dump the logical plan with pushdown=false for comparison.
     */
    @Test
    public void testDumpPlanWithPushdownDisabled()
    {
        Session pushdownOff = pushdownSession(false);
        Plan plan = plan("SELECT COUNT(*) FROM " + TABLE, pushdownOff);
        System.out.println("[PLAN - pushdown=false]\n" +
                textLogicalPlan(plan.getRoot(), plan.getTypes(), StatsAndCosts.empty(),
                        getQueryRunner().getMetadata().getFunctionAndTypeManager(), pushdownOff, 0));
    }

    // ---

    private Session pushdownSession(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(enabled))
                .build();
    }
}
