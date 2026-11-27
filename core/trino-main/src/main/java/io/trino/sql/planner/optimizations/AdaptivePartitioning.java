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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer is responsible for changing the partition count of hash partitioned fragments
 * at runtime. This uses the runtime output stats from FTE to determine estimated memory consumption
 * and changes the partition count if the estimated memory consumption is higher than the
 * runtimeAdaptivePartitioningMaxTaskSizeInBytes * current partitionCount.
 */
public class AdaptivePartitioning
        implements AdaptivePlanOptimizer
{
    private static final Logger log = Logger.get(AdaptivePartitioning.class);

    @Override
    public Result optimizeAndMarkPlanChanges(PlanNode plan, Context context)
    {
        // Skip if runtime adaptive partitioning is not enabled
        if (!isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled(context.session())) {
            return new Result(plan, ImmutableSet.of());
        }

        if (searchFrom(plan)
                .where(node -> node instanceof TableExecuteNode)
                .findFirst()
                .isPresent()) {
            // TableExecute is not supported
            return new Result(plan, ImmutableSet.of());
        }

        int maxPartitionCount = getFaultTolerantExecutionMaxPartitionCount(context.session());
        int runtimeAdaptivePartitioningPartitionCount = getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount(context.session());
        long runtimeAdaptivePartitioningMaxTaskSizeInBytes = getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize(context.session()).toBytes();
        RuntimeInfoProvider runtimeInfoProvider = context.runtimeInfoProvider();
        List<PlanFragment> fragments = runtimeInfoProvider.getAllPlanFragments();

        // Skip if there are already some fragments with the maximum partition count. This is to avoid re-planning
        // since currently we apply this rule on the entire plan. Once, we have a granular way of applying this rule,
        // we can remove this check.
        if (fragments.stream()
                .anyMatch(fragment ->
                        fragment.getPartitionCount().orElse(maxPartitionCount) >= runtimeAdaptivePartitioningPartitionCount)) {
            return new Result(plan, ImmutableSet.of());
        }

        for (PlanFragment fragment : fragments) {
            // Skip if the stage is not consuming hash partitioned input or if the runtime stats are accurate which
            // basically means that the stage can't be re-planned in the current implementation of AdaptivePlaner.
            // TODO: We need add an ability to re-plan fragment whose stats are estimated by progress.
            if (!consumesHashPartitionedInput(fragment) || runtimeInfoProvider.getRuntimeOutputStats(fragment.getId()).isAccurate()) {
                continue;
            }

            int partitionCount = fragment.getPartitionCount().orElse(maxPartitionCount);
            long estimatedMemoryConsumptionInBytes = estimateMemoryConsumption(fragment.getRoot(), runtimeInfoProvider);

            if (estimatedMemoryConsumptionInBytes > runtimeAdaptivePartitioningMaxTaskSizeInBytes * partitionCount) {
                log.info("Stage %s.%s has an estimated memory consumption of %s, changing partition count from %s to %s",
                        context.session().getQueryId(),
                        fragment.getId(),
                        succinctBytes(estimatedMemoryConsumptionInBytes),
                        partitionCount,
                        runtimeAdaptivePartitioningPartitionCount);
                Rewriter rewriter = new Rewriter(runtimeAdaptivePartitioningPartitionCount, context.idAllocator(), runtimeInfoProvider);
                PlanNode planNode = rewriteWith(rewriter, plan);
                return new Result(planNode, rewriter.getChangedPlanIds());
            }
        }

        return new Result(plan, ImmutableSet.of());
    }

    /**
     * Estimates the memory a fragment needs from the runtime size of its remote inputs (https://github.com/trinodb/trino/issues/18698).
     * Input bytes count once they reach an operator that holds its input, such as the build side of a join or an aggregation.
     * Operators not known to stream, such as a window or a sort, are assumed to hold their input. Bytes that only stream
     * through, such as the probe side of a join, do not count.
     */
    @VisibleForTesting
    static long estimateMemoryConsumption(PlanNode root, RuntimeInfoProvider runtimeInfoProvider)
    {
        return root.accept(new MemoryEstimator(runtimeInfoProvider), null).retainedBytes();
    }

    public static boolean consumesHashPartitionedInput(PlanFragment fragment)
    {
        return isPartitioned(fragment.getPartitioning());
    }

    private static boolean isPartitioned(PartitioningHandle partitioningHandle)
    {
        return partitioningHandle.equals(FIXED_HASH_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_HASH_DISTRIBUTION);
    }

    /**
     * @param streamedBytes estimated bytes a subtree outputs
     * @param retainedBytes estimated bytes held in memory by operators in the subtree
     */
    private record MemoryEstimate(long streamedBytes, long retainedBytes)
    {
        private static final MemoryEstimate NONE = new MemoryEstimate(0, 0);

        public MemoryEstimate plus(MemoryEstimate other)
        {
            return new MemoryEstimate(streamedBytes + other.streamedBytes, retainedBytes + other.retainedBytes);
        }

        // The operator holds all of its input, and its output is taken to be as large as its input
        public MemoryEstimate retainInput()
        {
            return new MemoryEstimate(streamedBytes, retainedBytes + streamedBytes);
        }

        public MemoryEstimate withoutOutput()
        {
            return new MemoryEstimate(0, retainedBytes);
        }
    }

    private static class MemoryEstimator
            extends PlanVisitor<MemoryEstimate, Void>
    {
        private final RuntimeInfoProvider runtimeInfoProvider;

        private MemoryEstimator(RuntimeInfoProvider runtimeInfoProvider)
        {
            this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }

        // Operators not handled below are assumed to hold all of their input
        @Override
        protected MemoryEstimate visitPlan(PlanNode node, Void context)
        {
            return visitSources(node).retainInput();
        }

        @Override
        public MemoryEstimate visitRemoteSource(RemoteSourceNode node, Void context)
        {
            // skip for replicate exchange since it's assumed that broadcast join will be chosen by
            // static optimizer only if build size is small.
            // TODO: Fix this assumption by using runtime stats
            if (node.getExchangeType() == REPLICATE) {
                return MemoryEstimate.NONE;
            }
            long bytes = 0;
            for (PlanFragmentId sourceFragmentId : node.getSourceFragmentIds()) {
                bytes += runtimeInfoProvider.getRuntimeOutputStats(sourceFragmentId).outputDataSizeEstimate().getTotalSizeInBytes();
            }
            return new MemoryEstimate(bytes, 0);
        }

        @Override
        public MemoryEstimate visitJoin(JoinNode node, Void context)
        {
            return join(node.getLeft(), node.getRight());
        }

        @Override
        public MemoryEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return node.getSource().accept(this, context).plus(node.getFilteringSource().accept(this, context).retainInput().withoutOutput());
        }

        @Override
        public MemoryEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return join(node.getLeft(), node.getRight());
        }

        @Override
        public MemoryEstimate visitIndexJoin(IndexJoinNode node, Void context)
        {
            return join(node.getProbeSource(), node.getIndexSource());
        }

        @Override
        public MemoryEstimate visitGroupId(GroupIdNode node, Void context)
        {
            // each input row is output once for every grouping set
            MemoryEstimate source = visitSources(node);
            return new MemoryEstimate(source.streamedBytes() * node.getGroupingSets().size(), source.retainedBytes());
        }

        @Override
        public MemoryEstimate visitAggregation(AggregationNode node, Void context)
        {
            MemoryEstimate source = node.getSource().accept(this, context);
            // partial aggregation memory is bounded by max_partial_aggregation_memory
            if (node.getStep() == PARTIAL) {
                return source;
            }
            return source.retainInput();
        }

        @Override
        public MemoryEstimate visitTopN(TopNNode node, Void context)
        {
            // holds at most count rows
            return new MemoryEstimate(0, node.getSource().accept(this, context).retainedBytes());
        }

        @Override
        public MemoryEstimate visitAdaptivePlanNode(AdaptivePlanNode node, Void context)
        {
            return node.getCurrentPlan().accept(this, context);
        }

        @Override
        public MemoryEstimate visitExchange(ExchangeNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitProject(ProjectNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitFilter(FilterNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitLimit(LimitNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitOutput(OutputNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitOffset(OffsetNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitSample(SampleNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return visitSources(node);
        }

        // UNNEST usually runs below the exchange, so the remote input size already includes its output
        @Override
        public MemoryEstimate visitUnnest(UnnestNode node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return visitSources(node);
        }

        @Override
        public MemoryEstimate visitDynamicFilterSource(DynamicFilterSourceNode node, Void context)
        {
            return visitSources(node);
        }

        // The build side is held, and the output is taken to be as large as both inputs, ignoring rows the join may add
        private MemoryEstimate join(PlanNode probe, PlanNode build)
        {
            return probe.accept(this, null).plus(build.accept(this, null).retainInput());
        }

        private MemoryEstimate visitSources(PlanNode node)
        {
            MemoryEstimate estimate = MemoryEstimate.NONE;
            for (PlanNode source : node.getSources()) {
                estimate = estimate.plus(source.accept(this, null));
            }
            return estimate;
        }
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final int partitionCount;
        private final PlanNodeIdAllocator idAllocator;
        private final RuntimeInfoProvider runtimeInfoProvider;
        private final Set<PlanNodeId> changedPlanIds = new HashSet<>();

        private Rewriter(int partitionCount, PlanNodeIdAllocator idAllocator, RuntimeInfoProvider runtimeInfoProvider)
        {
            this.partitionCount = partitionCount;
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            if (node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                // the result of the subtree will be broadcast, then no need to change partition count for the subtree
                // as the planner will only broadcast fragment output if it sees input data is small
                // or filter ratio is high
                return node;
            }
            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            PartitioningScheme partitioningScheme = node.getPartitioningScheme();

            // for FTE it only makes sense to set partition count for hash partitioned fragments
            if (node.getScope() == REMOTE
                    && node.getPartitioningScheme().getPartitioning().getHandle() == FIXED_HASH_DISTRIBUTION) {
                partitioningScheme = partitioningScheme.withPartitionCount(OptionalInt.of(partitionCount));
                changedPlanIds.add(node.getId());
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    sources,
                    node.getInputs(),
                    node.getOrderingScheme());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            if (node.getExchangeType() != REPARTITION) {
                return node;
            }

            Optional<PartitioningScheme> sourcePartitioningScheme = node.getSourceFragmentIds().stream()
                    .map(runtimeInfoProvider::getPlanFragment)
                    .map(PlanFragment::getOutputPartitioningScheme)
                    .filter(scheme -> isPartitioned(scheme.getPartitioning().getHandle()))
                    .findFirst();

            if (sourcePartitioningScheme.isEmpty()) {
                return node;
            }

            PartitioningScheme newPartitioningSchema = sourcePartitioningScheme.get()
                    .withPartitionCount(OptionalInt.of(partitionCount))
                    .withPartitioningHandle(FIXED_HASH_DISTRIBUTION);

            PlanNodeId nodeId = idAllocator.getNextId();
            changedPlanIds.add(nodeId);
            return new ExchangeNode(
                    nodeId,
                    REPARTITION,
                    REMOTE,
                    newPartitioningSchema,
                    ImmutableList.of(node),
                    ImmutableList.of(node.getOutputSymbols()),
                    node.getOrderingScheme());
        }

        public Set<PlanNodeId> getChangedPlanIds()
        {
            return ImmutableSet.copyOf(changedPlanIds);
        }
    }
}
