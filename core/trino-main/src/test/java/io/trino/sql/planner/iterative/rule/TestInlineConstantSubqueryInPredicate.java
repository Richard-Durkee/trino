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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.ApplyNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

/**
 * Tests for InlineConstantSubqueryInPredicate rule.
 * This rule transforms IN predicates with constant-valued subqueries into IN predicates with literal lists.
 *
 * Note: Positive test cases (rule fires) are difficult to test in unit tests because they require
 * ValuesNode with actual constant rows, which are typically created during query planning.
 * The functionality is thoroughly tested via integration tests with actual SQL queries.
 */
public class TestInlineConstantSubqueryInPredicate
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireForCorrelatedSubquery()
    {
        // Should not fire when correlation list is not empty
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol custkey = p.symbol("custkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(custkey), // correlation - not empty
                            p.values(orderkey, custkey),
                            p.values(id));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForEmptyValues()
    {
        // Should not fire for empty constant lists (no rows to inline)
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.values(ImmutableList.of(id), ImmutableList.of()));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMultipleSubqueryAssignments()
    {
        // Should only handle single IN predicate
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol custkey = p.symbol("custkey", BIGINT);
                    Symbol id1 = p.symbol("id1", BIGINT);
                    Symbol id2 = p.symbol("id2", BIGINT);
                    Symbol result1 = p.symbol("result1", BIGINT);
                    Symbol result2 = p.symbol("result2", BIGINT);

                    return p.apply(
                            ImmutableMap.of(
                                    result1, new ApplyNode.In(orderkey, id1),
                                    result2, new ApplyNode.In(custkey, id2)),
                            ImmutableList.of(),
                            p.values(orderkey, custkey),
                            p.values(id1, id2));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForExistsSubquery()
    {
        // Should only handle IN predicates, not EXISTS
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.Exists()),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.values(id));
                })
                .doesNotFire();
    }

    @Test
    public void testInlinesBigintValues()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(BIGINT, 1L),
                                            new Constant(BIGINT, 2L),
                                            new Constant(BIGINT, 3L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Constant(BIGINT, 1L),
                                                new Constant(BIGINT, 2L),
                                                new Constant(BIGINT, 3L))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesWithNull()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(BIGINT, 1L),
                                            new Constant(BIGINT, null),
                                            new Constant(BIGINT, 3L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Constant(BIGINT, 1L),
                                                new Constant(BIGINT, null),
                                                new Constant(BIGINT, 3L))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesWithTypeCasting()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", INTEGER);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(INTEGER, 1L),
                                            new Constant(INTEGER, 2L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Cast(new Constant(INTEGER, 1L), BIGINT),
                                                new Cast(new Constant(INTEGER, 2L), BIGINT))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesVarchar()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol name = p.symbol("name", VARCHAR);
                    Symbol val = p.symbol("val", VARCHAR);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(name, val)),
                            ImmutableList.of(),
                            p.values(1, name),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(VARCHAR, io.airlift.slice.Slices.utf8Slice("Alice")),
                                            new Constant(VARCHAR, io.airlift.slice.Slices.utf8Slice("Bob")))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(VARCHAR, "name"),
                                        ImmutableList.of(
                                                new Constant(VARCHAR, io.airlift.slice.Slices.utf8Slice("Alice")),
                                                new Constant(VARCHAR, io.airlift.slice.Slices.utf8Slice("Bob")))))),
                                values("name")));
    }

    @Test
    public void testInlinesDate()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    Symbol val = p.symbol("val", DATE);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderdate, val)),
                            ImmutableList.of(),
                            p.values(1, orderdate),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(DATE, 19000L),
                                            new Constant(DATE, 19001L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(DATE, "orderdate"),
                                        ImmutableList.of(
                                                new Constant(DATE, 19000L),
                                                new Constant(DATE, 19001L))))),
                                values("orderdate")));
    }

    @Test
    public void testInlinesDecimal()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol price = p.symbol("price", createDecimalType(10, 2));
                    Symbol val = p.symbol("val", createDecimalType(10, 2));
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(price, val)),
                            ImmutableList.of(),
                            p.values(1, price),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(createDecimalType(10, 2), 100L),
                                            new Constant(createDecimalType(10, 2), 200L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(createDecimalType(10, 2), "price"),
                                        ImmutableList.of(
                                                new Constant(createDecimalType(10, 2), 100L),
                                                new Constant(createDecimalType(10, 2), 200L))))),
                                values("price")));
    }

    @Test
    public void testInlinesDouble()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol price = p.symbol("price", DOUBLE);
                    Symbol val = p.symbol("val", DOUBLE);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(price, val)),
                            ImmutableList.of(),
                            p.values(1, price),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(DOUBLE, 1.5),
                                            new Constant(DOUBLE, 2.5))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(DOUBLE, "price"),
                                        ImmutableList.of(
                                                new Constant(DOUBLE, 1.5),
                                                new Constant(DOUBLE, 2.5))))),
                                values("price")));
    }

    @Test
    public void testInlinesBoolean()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol flag = p.symbol("flag", BOOLEAN);
                    Symbol val = p.symbol("val", BOOLEAN);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(flag, val)),
                            ImmutableList.of(),
                            p.values(1, flag),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(BOOLEAN, true),
                                            new Constant(BOOLEAN, false))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BOOLEAN, "flag"),
                                        ImmutableList.of(
                                                new Constant(BOOLEAN, true),
                                                new Constant(BOOLEAN, false))))),
                                values("flag")));
    }

    @Test
    public void testInlinesSmallint()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol value = p.symbol("value", SMALLINT);
                    Symbol val = p.symbol("val", SMALLINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(value, val)),
                            ImmutableList.of(),
                            p.values(1, value),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(SMALLINT, 1L),
                                            new Constant(SMALLINT, 2L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(SMALLINT, "value"),
                                        ImmutableList.of(
                                                new Constant(SMALLINT, 1L),
                                                new Constant(SMALLINT, 2L))))),
                                values("value")));
    }

    @Test
    public void testInlinesReal()
    {
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol value = p.symbol("value", REAL);
                    Symbol val = p.symbol("val", REAL);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(value, val)),
                            ImmutableList.of(),
                            p.values(1, value),
                            p.valuesOfExpressions(
                                    ImmutableList.of(val),
                                    ImmutableList.of(
                                            new Constant(REAL, (long) Float.floatToIntBits(1.5f)),
                                            new Constant(REAL, (long) Float.floatToIntBits(2.5f)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(REAL, "value"),
                                        ImmutableList.of(
                                                new Constant(REAL, (long) Float.floatToIntBits(1.5f)),
                                                new Constant(REAL, (long) Float.floatToIntBits(2.5f)))))),
                                values("value")));
    }

    @Test
    public void testInlinesSingleValue()
    {
        // Test with just one constant value
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(new Constant(BIGINT, 42L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(new Constant(BIGINT, 42L))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesDuplicateValues()
    {
        // Test with duplicate constant values (should preserve duplicates)
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(BIGINT, 1L),
                                            new Constant(BIGINT, 1L),
                                            new Constant(BIGINT, 2L),
                                            new Constant(BIGINT, 2L))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Constant(BIGINT, 1L),
                                                new Constant(BIGINT, 1L),
                                                new Constant(BIGINT, 2L),
                                                new Constant(BIGINT, 2L))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesAllNulls()
    {
        // Test with all NULL values
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", BIGINT);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(BIGINT, null),
                                            new Constant(BIGINT, null))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Constant(BIGINT, null),
                                                new Constant(BIGINT, null))))),
                                values("orderkey")));
    }

    @Test
    public void testInlinesNullWithTypeCasting()
    {
        // Test NULL constant with type casting
        tester().assertThat(new InlineConstantSubqueryInPredicate())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol id = p.symbol("id", INTEGER);
                    Symbol subqueryResult = p.symbol("subquery_result", BOOLEAN);

                    return p.apply(
                            ImmutableMap.of(subqueryResult, new ApplyNode.In(orderkey, id)),
                            ImmutableList.of(),
                            p.values(1, orderkey),
                            p.valuesOfExpressions(
                                    ImmutableList.of(id),
                                    ImmutableList.of(
                                            new Constant(INTEGER, 1L),
                                            new Constant(INTEGER, null))));
                })
                .matches(
                        project(
                                ImmutableMap.of("subquery_result", expression(new In(
                                        new Reference(BIGINT, "orderkey"),
                                        ImmutableList.of(
                                                new Cast(new Constant(INTEGER, 1L), BIGINT),
                                                new Constant(BIGINT, null))))),
                                values("orderkey")));
    }
}
