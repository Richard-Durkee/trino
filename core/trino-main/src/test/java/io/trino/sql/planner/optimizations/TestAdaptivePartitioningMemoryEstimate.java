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
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableLongArray;
import io.trino.cost.StaticRuntimeInfoProvider;
import io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.optimizations.AdaptivePartitioning.estimateMemoryConsumption;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAdaptivePartitioningMemoryEstimate
{
    private static final long PROBE_BYTES = 100;
    private static final long BUILD_BYTES = 10;

    private static final StaticRuntimeInfoProvider RUNTIME_INFO = new StaticRuntimeInfoProvider(
            ImmutableMap.of(
                    new PlanFragmentId("probe"), stats(PROBE_BYTES),
                    new PlanFragmentId("build"), stats(BUILD_BYTES)),
            ImmutableMap.of());

    private final PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
    private final Symbol probeKey = p.symbol("probe_key", BIGINT);
    private final Symbol buildKey = p.symbol("build_key", BIGINT);

    @Test
    public void testJoinHoldsBuildSide()
    {
        assertThat(estimate(join(probe(), build()))).isEqualTo(BUILD_BYTES);
    }

    @Test
    public void testJoinHoldsLargerBuildSide()
    {
        // the larger input is on the build side
        PlanNode join = p.join(INNER, build(), probe(), new EquiJoinClause(buildKey, probeKey));
        assertThat(estimate(join)).isEqualTo(PROBE_BYTES);
    }

    @Test
    public void testJoinOnBuildSideOfJoin()
    {
        // the inner join holds its build side, and the outer join holds the output of the inner join
        Symbol otherKey = p.symbol("other_key", BIGINT);
        PlanNode innerJoin = join(probe(), build());
        PlanNode outerJoin = p.join(INNER, remoteSource("probe", otherKey, REPARTITION), innerJoin, new EquiJoinClause(otherKey, probeKey));
        assertThat(estimate(outerJoin)).isEqualTo(BUILD_BYTES + PROBE_BYTES + BUILD_BYTES);
    }

    @Test
    public void testBroadcastBuildSide()
    {
        assertThat(estimate(join(probe(), remoteSource("build", buildKey, REPLICATE)))).isZero();
    }

    @Test
    public void testSemiJoinHoldsFilteringSource()
    {
        Symbol match = p.symbol("match", BIGINT);
        assertThat(estimate(p.semiJoin(probeKey, buildKey, match, probe(), build()))).isEqualTo(BUILD_BYTES);
    }

    @Test
    public void testAggregationHoldsInput()
    {
        assertThat(estimate(aggregation(Step.FINAL, probe()))).isEqualTo(PROBE_BYTES);
        assertThat(estimate(aggregation(Step.SINGLE, probe()))).isEqualTo(PROBE_BYTES);
        assertThat(estimate(aggregation(Step.PARTIAL, probe()))).isZero();
    }

    @Test
    public void testAggregationOverJoin()
    {
        // the build side is held by the join, and the join output, taken as both inputs, by the aggregation
        assertThat(estimate(aggregation(Step.SINGLE, join(probe(), build())))).isEqualTo(BUILD_BYTES + PROBE_BYTES + BUILD_BYTES);
    }

    @Test
    public void testStreamingOperators()
    {
        PlanNode plan = p.output(output -> output.source(
                p.project(Assignments.identity(probeKey),
                        p.exchange(exchange -> exchange
                                .scope(ExchangeNode.Scope.LOCAL)
                                .singleDistributionPartitioningScheme(probeKey)
                                .addInputsSet(probeKey)
                                .addSource(probe())))));
        assertThat(estimate(plan)).isZero();
    }

    @Test
    public void testOtherOperatorsHoldInput()
    {
        assertThat(estimate(p.sort(ImmutableList.of(probeKey), probe()))).isEqualTo(PROBE_BYTES);
        assertThat(estimate(p.tableWriter(ImmutableList.of(probeKey), ImmutableList.of("column"), probe()))).isEqualTo(PROBE_BYTES);
    }

    @Test
    public void testTopNHoldsOnlyItsRows()
    {
        assertThat(estimate(p.topN(10, ImmutableList.of(probeKey), join(probe(), build())))).isEqualTo(BUILD_BYTES);
    }

    @Test
    public void testGroupIdMultipliesInput()
    {
        Symbol groupId = p.symbol("group_id", BIGINT);
        PlanNode groupIdNode = p.groupId(ImmutableList.of(ImmutableList.of(probeKey), ImmutableList.of()), ImmutableList.of(), groupId, probe());
        assertThat(estimate(groupIdNode)).isZero();
        assertThat(estimate(aggregation(Step.SINGLE, groupIdNode))).isEqualTo(2 * PROBE_BYTES);
    }

    @Test
    public void testAdaptivePlanNodeUsesCurrentPlan()
    {
        // the initial plan is not counted
        PlanNode adaptive = new AdaptivePlanNode(new PlanNodeId("adaptive"), build(), ImmutableSet.of(buildKey), probe());
        assertThat(estimate(aggregation(Step.SINGLE, adaptive))).isEqualTo(PROBE_BYTES);
    }

    private long estimate(PlanNode plan)
    {
        return estimateMemoryConsumption(plan, RUNTIME_INFO);
    }

    private PlanNode join(PlanNode probe, PlanNode build)
    {
        return p.join(INNER, probe, build, new EquiJoinClause(probeKey, buildKey));
    }

    private PlanNode aggregation(Step step, PlanNode source)
    {
        return p.aggregation(aggregation -> aggregation
                .singleGroupingSet(probeKey)
                .step(step)
                .source(source));
    }

    private RemoteSourceNode probe()
    {
        return remoteSource("probe", probeKey, REPARTITION);
    }

    private RemoteSourceNode build()
    {
        return remoteSource("build", buildKey, REPARTITION);
    }

    private RemoteSourceNode remoteSource(String fragmentId, Symbol output, ExchangeNode.Type exchangeType)
    {
        return p.remoteSource(ImmutableList.of(new PlanFragmentId(fragmentId)), ImmutableList.of(output), Optional.empty(), exchangeType, TASK);
    }

    private static OutputStatsEstimateResult stats(long bytes)
    {
        return new OutputStatsEstimateResult(ImmutableLongArray.of(bytes), 1, "FINISHED", true);
    }
}
