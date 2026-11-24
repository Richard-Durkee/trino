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
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPropertyDerivations
        extends BasePlanTest
{
    private final PlannerContext plannerContext = plannerContextBuilder().build();
    private final Symbol a = new Symbol(BIGINT, "a");
    private final Symbol b = new Symbol(BIGINT, "b");
    private final Symbol c = new Symbol(BIGINT, "c");
    private final Symbol aGroup = new Symbol(BIGINT, "a_gid");
    private final Symbol bGroup = new Symbol(BIGINT, "b_gid");
    private final Symbol groupId = new Symbol(BIGINT, "groupid");

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build());
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testUnorderedDropsPropertiesThatFollowOrderSensitiveOnes()
    {
        // AssignUniqueId declares the columns of its source constant within a group of the id
        List<LocalProperty<Symbol>> properties = ImmutableList.of(new GroupingProperty<>(ImmutableList.of(a)), new ConstantProperty<>(b));
        assertThat(ActualProperties.builder().local(properties).unordered(false).build().getLocalProperties())
                .containsExactly(new GroupingProperty<>(ImmutableList.of(a)), new ConstantProperty<>(b));
        assertThat(ActualProperties.builder().local(properties).unordered(true).build().getLocalProperties()).isEmpty();

        // constants that do not depend on order-sensitive properties stay
        assertThat(ActualProperties.builder()
                .local(ImmutableList.of(new ConstantProperty<>(c), new SortingProperty<>(a, ASC_NULLS_FIRST), new ConstantProperty<>(b)))
                .unordered(true)
                .build()
                .getLocalProperties())
                .containsExactly(new ConstantProperty<>(c));
    }

    @Test
    public void testGroupIdWithSeveralGroupingSetsDoesNotKeepGroupingAndSorting()
    {
        List<LocalProperty<Symbol>> input = ImmutableList.of(new SortingProperty<>(a, ASC_NULLS_FIRST));
        GroupIdNode severalSets = groupId(ImmutableList.of(ImmutableList.of(aGroup), ImmutableList.of(aGroup, bGroup)));
        ActualProperties properties = derive(severalSets, ActualProperties.builder().local(input).build());
        assertThat(properties.getLocalProperties()).isEmpty();

        // with one grouping set the input is passed through once
        GroupIdNode oneSet = groupId(ImmutableList.of(ImmutableList.of(aGroup)));
        assertThat(derive(oneSet, ActualProperties.builder().local(input).build()).getLocalProperties())
                .containsExactly(new SortingProperty<>(aGroup, ASC_NULLS_FIRST));
    }

    @Test
    public void testGroupIdWithSeveralGroupingSetsIsNotOrderedStream()
    {
        ValuesNode values = new ValuesNode(new PlanNodeId("values"), ImmutableList.of(a, b));
        StreamProperties valuesProperties = StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(values, ImmutableList.of(), plannerContext.getMetadata(), getPlanTester().getDefaultSession());
        SortNode sort = new SortNode(new PlanNodeId("sort"), values, new OrderingScheme(ImmutableList.of(a), Map.of(a, ASC_NULLS_FIRST)), false);
        StreamProperties ordered = StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(sort, ImmutableList.of(valuesProperties), plannerContext.getMetadata(), getPlanTester().getDefaultSession());
        assertThat(ordered.isOrdered()).isTrue();

        StreamProperties oneSet = derive(groupId(ImmutableList.of(ImmutableList.of(aGroup))), ordered);
        assertThat(oneSet.isOrdered()).isTrue();
        StreamProperties severalSets = derive(groupId(ImmutableList.of(ImmutableList.of(aGroup), ImmutableList.of(aGroup, bGroup))), ordered);
        assertThat(severalSets.isOrdered()).isFalse();
    }

    @Test
    public void testGroupIdOverWindowInPlan()
    {
        // the window groups its output on custkey, which GroupId does not keep when it repeats its input for each grouping set
        String query = "SELECT custkey, orderstatus, count(DISTINCT orderpriority), max(rn) " +
                "FROM (SELECT custkey, orderstatus, orderpriority, row_number() OVER (PARTITION BY custkey ORDER BY orderkey) rn FROM orders) " +
                "GROUP BY GROUPING SETS ((custkey), (custkey, orderstatus))";
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .build();
        // a plan of a single node, so that the properties can be derived from the whole plan
        Plan plan = plan(query, OPTIMIZED_AND_VALIDATED, true);
        List<GroupIdNode> groupIds = searchFrom(plan.getRoot()).where(GroupIdNode.class::isInstance).findAll().stream()
                .map(GroupIdNode.class::cast)
                .collect(toImmutableList());
        assertThat(groupIds).hasSize(1);
        GroupIdNode groupIdNode = groupIds.getFirst();

        getPlanTester().inTransaction(session, transactionSession -> {
            // registers the catalog for the transaction
            getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(transactionSession, "tpch");
            ActualProperties input = PropertyDerivations.derivePropertiesRecursively(groupIdNode.getSource(), getPlanTester().getPlannerContext(), transactionSession, new SymbolAllocator(ImmutableList.of()));
            assertThat(input.getLocalProperties()).anyMatch(GroupingProperty.class::isInstance);
            ActualProperties output = PropertyDerivations.derivePropertiesRecursively(groupIdNode, getPlanTester().getPlannerContext(), transactionSession, new SymbolAllocator(ImmutableList.of()));
            assertThat(output.getLocalProperties()).noneMatch(GroupingProperty.class::isInstance).noneMatch(SortingProperty.class::isInstance);
            return null;
        });
    }

    private GroupIdNode groupId(List<List<Symbol>> groupingSets)
    {
        return new GroupIdNode(
                new PlanNodeId("groupId"),
                new ValuesNode(new PlanNodeId("values"), ImmutableList.of(a, b)),
                groupingSets,
                ImmutableMap.of(aGroup, a, bGroup, b),
                ImmutableList.of(),
                groupId);
    }

    private ActualProperties derive(GroupIdNode node, ActualProperties input)
    {
        return PropertyDerivations.deriveProperties(node, ImmutableList.of(input), plannerContext, getPlanTester().getDefaultSession(), new SymbolAllocator(ImmutableList.of()));
    }

    private StreamProperties derive(GroupIdNode node, StreamProperties input)
    {
        return StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(node, ImmutableList.of(input), plannerContext.getMetadata(), getPlanTester().getDefaultSession());
    }
}
