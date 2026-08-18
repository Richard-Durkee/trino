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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class MarkDistinctOperator
        implements Operator
{
    // Once a single cold partition's spilled data (seed keys + raw rows) exceeds this size, resolving it
    // is deferred one level deeper via recursive re-partitioning instead of loading it directly.
    private static final DataSize DEFAULT_MAX_PARTITION_RESOLVE_SIZE = DataSize.of(64, DataSize.Unit.MEGABYTE);

    public static class MarkDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> markDistinctChannels;
        private final List<Type> types;
        private final FlatHashStrategyCompiler hashStrategyCompiler;
        private final boolean spillEnabled;
        private final SpillerFactory spillerFactory;
        private boolean closed;

        public MarkDistinctOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                Collection<Integer> markDistinctChannels,
                FlatHashStrategyCompiler hashStrategyCompiler,
                boolean spillEnabled,
                SpillerFactory spillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.markDistinctChannels = ImmutableList.copyOf(requireNonNull(markDistinctChannels, "markDistinctChannels is null"));
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.types = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BOOLEAN)
                    .build();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MarkDistinctOperator.class.getSimpleName());
            return new MarkDistinctOperator(operatorContext, types, markDistinctChannels, hashStrategyCompiler, spillEnabled, spillerFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MarkDistinctOperatorFactory(operatorId, planNodeId, types.subList(0, types.size() - 1), markDistinctChannels, hashStrategyCompiler, spillEnabled, spillerFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final SpillableMarkDistinctHash markDistinctHash;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final boolean spillEnabled;
    private final int[] markDistinctChannels;

    private Page inputPage;
    private boolean finishing;

    // for yield when memory is not available
    private Work<Page> unfinishedWork;

    // finish()-time drain of any partitions that were spilled while consuming input
    private boolean drainingStarted;
    private boolean drainingDone;

    public MarkDistinctOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> markDistinctChannels,
            FlatHashStrategyCompiler hashStrategyCompiler,
            boolean spillEnabled,
            SpillerFactory spillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        requireNonNull(markDistinctChannels, "markDistinctChannels is null");

        List<Type> rawTypes = types.subList(0, types.size() - 1); // drop the trailing marker type
        ImmutableList.Builder<Type> distinctTypes = ImmutableList.builder();
        for (int channel : markDistinctChannels) {
            distinctTypes.add(types.get(channel));
        }
        this.markDistinctChannels = Ints.toArray(markDistinctChannels);
        this.spillEnabled = spillEnabled;

        this.markDistinctHash = new SpillableMarkDistinctHash(
                operatorContext.getSession(),
                distinctTypes.build(),
                rawTypes,
                this.markDistinctChannels,
                hashStrategyCompiler,
                this::updateMemoryReservation,
                spillEnabled,
                spillerFactory,
                operatorContext,
                DEFAULT_MAX_PARTITION_RESOLVE_SIZE);
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && !hasUnfinishedInput() && drainingDone;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !hasUnfinishedInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput());

        inputPage = page;

        unfinishedWork = markDistinctHash.markDistinctRows(page);
        updateMemoryReservation();
    }

    @Override
    public Page getOutput()
    {
        if (finishing && !hasUnfinishedInput()) {
            return drainSpilledPartitions();
        }

        if (unfinishedWork == null) {
            return null;
        }

        if (!unfinishedWork.process()) {
            return null;
        }

        // rows that landed in a spilled partition are excluded from this page: their marker is
        // resolved later, during the finish()-time drain
        Page outputPage = unfinishedWork.getResult();

        unfinishedWork = null;
        inputPage = null;

        updateMemoryReservation();
        return outputPage;
    }

    private Page drainSpilledPartitions()
    {
        if (drainingDone) {
            return null;
        }
        if (!drainingStarted) {
            drainingStarted = true;
            if (!markDistinctHash.hasSpilledPartitions()) {
                drainingDone = true;
                return null;
            }
        }
        Page page = markDistinctHash.pollNextDrainedPage();
        if (page == null) {
            drainingDone = true;
        }
        return page;
    }

    private boolean hasUnfinishedInput()
    {
        return inputPage != null || unfinishedWork != null;
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revocable memory should be reserved");
        return markDistinctHash.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke()
    {
        markDistinctHash.finishMemoryRevoke();
        updateMemoryReservation();
    }

    @Override
    public void close()
            throws IOException
    {
        markDistinctHash.close();
    }

    /**
     * Update memory usage.
     *
     * @return true to if the reservation is within the limit
     */
    // TODO: update in the interface now that the new memory tracking framework is landed
    // Essentially we would love to have clean interfaces to support both pushing and pulling memory usage
    // The following implementation is a hybrid model, where the push model is going to call the pull model causing reentrancy
    private boolean updateMemoryReservation()
    {
        long estimatedSize = markDistinctHash.getEstimatedSize();
        if (spillEnabled) {
            // Operator/driver will be blocked on memory after we call localRevocableMemoryContext.setBytes().
            // If memory is not available, once we return, this operator will be blocked until memory is
            // available, or startMemoryRevoke() is invoked to spill and free some of it.
            localRevocableMemoryContext.setBytes(estimatedSize);
        }
        else {
            localUserMemoryContext.setBytes(estimatedSize);
        }
        // If memory is not available, inform the caller that we cannot proceed for allocation.
        return operatorContext.isWaitingForMemory().isDone();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return markDistinctHash.getCapacity();
    }
}
