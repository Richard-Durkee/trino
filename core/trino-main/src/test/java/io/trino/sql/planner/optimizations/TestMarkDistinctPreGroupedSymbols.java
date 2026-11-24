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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMarkDistinctPreGroupedSymbols
        extends BasePlanTest
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider()
            .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER::getType)))
            .withKeyDeserializers(ImmutableMap.of(Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER)))
            .get();
    private static final String CORRELATED_SUBQUERY = "SELECT l.orderkey, (SELECT n.name FROM nation n WHERE n.nationkey = l.suppkey % 25) FROM lineitem l";

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build());
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testCorrelatedSubqueryWithReplicatedJoin()
    {
        // AssignUniqueId declares its id symbol grouped, a replicated join preserves the order of the probe side and
        // nothing between the join and MarkDistinct interleaves rows, so the check is streaming. This has to hold
        // for the default join distribution and with many drivers, where the source could have been repartitioned.
        for (String joinDistributionType : ImmutableList.of("AUTOMATIC", "BROADCAST")) {
            for (String taskConcurrency : ImmutableList.of("1", "16")) {
                List<MarkDistinctNode> nodes = markDistinctNodes(CORRELATED_SUBQUERY, joinDistributionType, taskConcurrency);
                assertThat(nodes).hasSize(1);
                MarkDistinctNode node = nodes.getFirst();
                assertThat(node.getPreGroupedSymbols()).isNotEmpty();
                assertThat(node.getDistinctSymbols()).containsAll(node.getPreGroupedSymbols());
                assertJoinIsDirectlyBelowWithoutExchange(node);
            }
        }
    }

    @Test
    public void testCorrelatedSubqueryWithPartitionedJoin()
    {
        // the remote exchange between the join and MarkDistinct gives no grouping guarantee
        for (String taskConcurrency : ImmutableList.of("1", "16")) {
            List<MarkDistinctNode> nodes = markDistinctNodes(CORRELATED_SUBQUERY, "PARTITIONED", taskConcurrency);
            assertThat(nodes).hasSize(1);
            assertThat(nodes.getFirst().getPreGroupedSymbols()).isEmpty();
            assertThat(nodes.getFirst().getSource()).isNotInstanceOf(JoinNode.class);
        }
    }

    @Test
    public void testCorrelatedSubqueryWithPartitionedJoinOnTheGroupedKey()
    {
        // the distinct symbols contain the join key, so the partitioned join output needs no exchange before MarkDistinct
        String query = "SELECT l.orderkey, (SELECT o.orderstatus FROM orders o WHERE o.orderkey = l.orderkey) FROM lineitem l";
        for (String joinDistributionType : ImmutableList.of("AUTOMATIC", "PARTITIONED")) {
            List<MarkDistinctNode> nodes = markDistinctNodes(query, joinDistributionType, "16");
            assertThat(nodes).hasSize(1);
            assertThat(nodes.getFirst().getPreGroupedSymbols()).isNotEmpty();
            assertJoinIsDirectlyBelowWithoutExchange(nodes.getFirst());
        }
    }

    private static void assertJoinIsDirectlyBelowWithoutExchange(MarkDistinctNode node)
    {
        PlanNode source = node.getSource();
        while (source instanceof ProjectNode project) {
            source = project.getSource();
        }
        assertThat(source).isInstanceOf(JoinNode.class);
        // only the build side is exchanged
        assertThat(searchFrom(((JoinNode) source).getLeft()).where(ExchangeNode.class::isInstance).findAll()).isEmpty();
    }

    @Test
    public void testMultipleDistinctAggregationsAreNotPreGrouped()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("distinct_aggregations_strategy", "mark_distinct")
                .build();
        SubPlan subPlan = subplan("SELECT count(DISTINCT custkey), count(DISTINCT orderstatus) FROM orders", OPTIMIZED_AND_VALIDATED, false, session);
        List<MarkDistinctNode> nodes = markDistinctNodes(subPlan);
        assertThat(nodes).hasSize(2);
        nodes.forEach(node -> assertThat(node.getPreGroupedSymbols()).isEmpty());
    }

    @Test
    public void testPreGroupedSymbolsAreEmptyOrAllDistinctSymbols()
    {
        Symbol a = new Symbol(BIGINT, "a");
        Symbol b = new Symbol(BIGINT, "b");
        Symbol marker = new Symbol(BOOLEAN, "marker");
        ValuesNode source = new ValuesNode(new PlanNodeId("values"), 1);
        assertThat(new MarkDistinctNode(new PlanNodeId("markDistinct"), source, marker, ImmutableList.of(a, b)).getPreGroupedSymbols()).isEmpty();
        assertThat(new MarkDistinctNode(new PlanNodeId("markDistinct"), source, marker, ImmutableList.of(a, b), ImmutableList.of(b, a)).getPreGroupedSymbols()).containsExactly(b, a);
        assertThatThrownBy(() -> new MarkDistinctNode(new PlanNodeId("markDistinct"), source, marker, ImmutableList.of(a, b), ImmutableList.of(b)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty or all the distinct symbols");
    }

    @Test
    public void testDistinctAggregationsOverGroupIdAreNotPreGrouped()
    {
        // GroupId repeats its input once per grouping set, so rows with equal values are not adjacent although its input is grouped
        String query = "SELECT custkey, orderstatus, count(DISTINCT orderpriority), count(DISTINCT clerk), max(rn) " +
                "FROM (SELECT custkey, orderstatus, orderpriority, clerk, row_number() OVER (PARTITION BY custkey ORDER BY orderkey) rn FROM orders) " +
                "GROUP BY GROUPING SETS ((custkey), (custkey, orderstatus))";
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .build();
        List<MarkDistinctNode> nodes = markDistinctNodes(subplan(query, OPTIMIZED_AND_VALIDATED, false, session));
        assertThat(nodes).hasSize(2);
        nodes.forEach(node -> assertThat(node.getPreGroupedSymbols()).isEmpty());
        // GroupId is directly below the mark distinct nodes, with nothing exchanged in between, which is what makes the case relevant
        assertThat(nodes.stream().anyMatch(node -> searchFrom(node.getSource())
                .recurseOnlyWhen(MarkDistinctNode.class::isInstance)
                .where(GroupIdNode.class::isInstance)
                .matches())).isTrue();
    }

    @Test
    public void testConstantsAreNotEvidenceOfGrouping()
    {
        Symbol a = new Symbol(BIGINT, "a");
        Symbol b = new Symbol(BIGINT, "b");
        Symbol unique = new Symbol(BIGINT, "unique");
        // grouping and sorting are evidence
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(unique), properties(new GroupingProperty<>(ImmutableList.of(unique))))).isTrue();
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a, b), properties(new SortingProperty<>(a, ASC_NULLS_FIRST), new SortingProperty<>(b, ASC_NULLS_FIRST)))).isTrue();
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a, unique), properties(new SortingProperty<>(a, ASC_NULLS_FIRST), new GroupingProperty<>(ImmutableList.of(unique))))).isTrue();
        // After a join that can spill dropped the grouping on the id, the columns that AssignUniqueId declared
        // constant within a group of the id are left as constants of the whole stream. They do not prove grouping.
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a), properties(new ConstantProperty<>(a)))).isFalse();
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a, b), properties(new ConstantProperty<>(a), new ConstantProperty<>(b)))).isFalse();
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a), properties(new GroupingProperty<>(ImmutableList.of(unique)), new ConstantProperty<>(a)))).isFalse();
        // nothing is known
        assertThat(AddLocalExchanges.isGroupedOnAll(ImmutableList.of(a), properties())).isFalse();
    }

    @Test
    public void testSerialization()
    {
        Symbol a = new Symbol(BIGINT, "a");
        Symbol b = new Symbol(BIGINT, "b");
        MarkDistinctNode node = new MarkDistinctNode(
                new PlanNodeId("markDistinct"),
                new ValuesNode(new PlanNodeId("values"), 1),
                new Symbol(BOOLEAN, "marker"),
                ImmutableList.of(a, b),
                ImmutableList.of(b, a));

        JsonCodec<PlanNode> codec = planNodeCodec();

        MarkDistinctNode copy = (MarkDistinctNode) codec.fromJson(codec.toJson(node));
        assertThat(copy.getDistinctSymbols()).isEqualTo(node.getDistinctSymbols());
        assertThat(copy.getPreGroupedSymbols()).isEqualTo(node.getPreGroupedSymbols());
        assertThat(copy.getMarkerSymbol()).isEqualTo(node.getMarkerSymbol());
        assertThat(((MarkDistinctNode) node.replaceChildren(ImmutableList.of(node.getSource()))).getPreGroupedSymbols()).containsExactly(b, a);
    }

    @SafeVarargs
    private static List<LocalProperty<Symbol>> properties(LocalProperty<Symbol>... properties)
    {
        return ImmutableList.copyOf(properties);
    }

    private static JsonCodec<PlanNode> planNodeCodec()
    {
        return new JsonCodecFactory(JSON_MAPPER).jsonCodec(PlanNode.class);
    }

    private List<MarkDistinctNode> markDistinctNodes(String sql, String joinDistributionType, String taskConcurrency)
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType)
                .setSystemProperty(TASK_CONCURRENCY, taskConcurrency)
                .build();
        return markDistinctNodes(subplan(sql, OPTIMIZED_AND_VALIDATED, false, session));
    }

    private static List<MarkDistinctNode> markDistinctNodes(SubPlan subPlan)
    {
        return subPlan.getAllFragments().stream()
                .map(PlanFragment::getRoot)
                .flatMap(root -> searchFrom(root).where(MarkDistinctNode.class::isInstance).findAll().stream())
                .map(MarkDistinctNode.class::cast)
                .collect(toImmutableList());
    }
}
