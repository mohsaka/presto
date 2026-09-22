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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DateType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static org.testng.Assert.assertNotNull;

/**
 * Reproduces the stack overflow that occurs when ExpressionConverter.toIcebergExpression()
 * is called with a Domain containing thousands of single-point values (e.g. all daily
 * created_date partitions of a large table).
 *
 * The buggy code at line 144 of ExpressionConverter.java left-folds each point value into:
 *   Or(Or(Or(alwaysFalse, eq(v0)), eq(v1)), eq(v2)) ...
 * producing a left-spine tree of depth N. Iceberg's ExpressionVisitors recurse through
 * this tree, overflowing the JVM stack for N >= ~3 000.
 *
 * The fix is to collect all single-point values and emit a single Expressions.in(col, values)
 * predicate (depth 1) instead of a chain of binary Or nodes.
 */
public class TestExpressionConverterStackOverflow
{
    private static final int LARGE_PARTITION_COUNT = 5_000;

    private static final IcebergColumnHandle CREATED_DATE_HANDLE = primitiveIcebergColumnHandle(
            1, "created_date", DateType.DATE, Optional.empty());

    /**
     * Verifies the fix: after switching to Expressions.in(), converting a 5 000-point Domain
     * produces a flat in() predicate (depth 1) that the recursive ExpressionVisitor handles
     * without overflowing. The old left-fold approach built Or(Or(Or(...))) of depth N — the
     * in() predicate is the correct replacement.
     */
    @Test
    public void testLargeMultiValueDomainDoesNotOverflowAfterFix()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(LARGE_PARTITION_COUNT));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);

        // Fixed code emits in() — depth 1 — should not overflow.
        String result = visitExpression(expression);
        assertNotNull(result);
    }

    /**
     * Verifies that using Expressions.in() (the fix) does NOT overflow for the same input.
     * Expressions.in() is depth-1 regardless of the number of values.
     */
    @Test
    public void testInExpressionDoesNotOverflow()
    {
        List<Long> values = buildDateValues(LARGE_PARTITION_COUNT);
        // The fixed code path: emit a single in() predicate instead of chained Or nodes
        Expression expression = Expressions.in("created_date", values);
        String result = visitExpression(expression);
        assertNotNull(result);
    }

    /**
     * Sanity check: a small domain (100 values) works with the current left-fold approach.
     */
    @Test
    public void testSmallDomainDoesNotOverflow()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(100));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        String result = visitExpression(expression);
        assertNotNull(result);
    }

    // --- helpers ---

    private static List<Long> buildDateValues(int count)
    {
        // Epoch-day values: 19000 = 2022-01-06, mimicking real created_date partition values
        return LongStream.range(19000, 19000 + count)
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Visits the expression using Iceberg's standard recursive ExpressionVisitor,
     * which is the same mechanism used internally by Iceberg's scan/filter evaluation.
     * Returns a string representation so the result can be asserted non-null.
     */
    private static String visitExpression(Expression expression)
    {
        return ExpressionVisitors.visit(expression, new ExpressionVisitor<String>()
        {
            @Override
            public String alwaysTrue()
            {
                return "TRUE";
            }

            @Override
            public String alwaysFalse()
            {
                return "FALSE";
            }

            @Override
            public String not(String result)
            {
                return "NOT(" + result + ")";
            }

            @Override
            public String and(String left, String right)
            {
                return "AND(" + left + "," + right + ")";
            }

            @Override
            public String or(String left, String right)
            {
                return "OR(" + left + "," + right + ")";
            }

            @Override
            public <T> String predicate(org.apache.iceberg.expressions.BoundPredicate<T> pred)
            {
                return pred.toString();
            }

            @Override
            public <T> String predicate(org.apache.iceberg.expressions.UnboundPredicate<T> pred)
            {
                return pred.toString();
            }
        });
    }
}
