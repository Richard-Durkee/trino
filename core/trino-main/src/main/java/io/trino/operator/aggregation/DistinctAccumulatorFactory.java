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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.MarkDistinctHash;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.aggregation.SpilledRows.Rows;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.aggregation.SpilledRows.readRows;
import static io.trino.operator.aggregation.SpilledRows.writeRows;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DistinctAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorFactory delegate;
    private final List<Type> argumentTypes;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final Session session;

    public DistinctAccumulatorFactory(
            AccumulatorFactory delegate,
            List<Type> argumentTypes,
            FlatHashStrategyCompiler hashStrategyCompiler,
            Session session)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return delegate.getLambdaInterfaces();
    }

    @Override
    public Accumulator createAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return new DistinctAccumulator(
                delegate.createAccumulator(lambdaProviders),
                argumentTypes,
                session,
                hashStrategyCompiler);
    }

    @Override
    public Accumulator createIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        throw new UnsupportedOperationException("Distinct aggregation is not decomposable");
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return new DistinctGroupedAccumulator(
                delegate.createGroupedAccumulator(lambdaProviders),
                argumentTypes,
                session,
                hashStrategyCompiler);
    }

    // Spilled state is the group's distinct rows, which a new instance deduplicates again in addIntermediate()
    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return createGroupedAccumulator(lambdaProviders);
    }

    public Type spillType()
    {
        return SpilledRows.spillType(argumentTypes);
    }

    @Override
    public AggregationMaskBuilder createAggregationMaskBuilder()
    {
        return delegate.createAggregationMaskBuilder();
    }

    private static class DistinctAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        @Nullable
        private MarkDistinctHash hash; // null after prepareFinal() is called

        private DistinctAccumulator(
                Accumulator accumulator,
                List<Type> inputTypes,
                Session session,
                FlatHashStrategyCompiler hashStrategyCompiler)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hash = new MarkDistinctHash(
                    session,
                    inputTypes,
                    hashStrategyCompiler,
                    UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = accumulator.getEstimatedSize();
            if (hash != null) {
                estimatedSize += hash.getEstimatedSize();
            }
            return estimatedSize;
        }

        @Override
        public Accumulator copy()
        {
            throw new UnsupportedOperationException("Distinct aggregation function state can not be copied");
        }

        @Override
        public void addInput(Page arguments, AggregationMask mask)
        {
            checkState(hash != null, "prepareFinal() has already been called");
            // 1. filter out positions based on mask, if present
            Page filtered = mask.filterPage(arguments);

            // 2. compute a distinct mask block
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. update original mask to the new distinct mask block
            mask.reset(filtered.getPositionCount());
            mask.applyMaskBlock(distinctMask);
            if (mask.isSelectNone()) {
                return;
            }

            // 4. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(filtered, mask);
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }

        @Override
        public boolean prepareFinal(UpdateMemory updateMemory)
        {
            // release hash memory after all inputs have been added
            hash = null;
            return accumulator.prepareFinal(updateMemory);
        }
    }

    private static class DistinctGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final List<Type> hashTypes;
        @Nullable
        private MarkDistinctHash hash; // null after prepareFinal() is called
        @Nullable
        private int[] groupDistinctRowStarts; // set by the first evaluateIntermediate()
        @Nullable
        private int[] groupDistinctRowIds; // distinct ids ordered by group, set by the first evaluateIntermediate()
        @Nullable
        private PageBuilder spillPageBuilder; // reused across groups by evaluateIntermediate()
        private int groupCount;

        private DistinctGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> inputTypes,
                Session session,
                FlatHashStrategyCompiler hashStrategyCompiler)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.hashTypes = ImmutableList.<Type>builder()
                    .add(INTEGER) // group id column
                    .addAll(inputTypes)
                    .build();
            this.hash = new MarkDistinctHash(
                    session,
                    hashTypes,
                    hashStrategyCompiler,
                    UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = accumulator.getEstimatedSize();
            if (hash != null) {
                estimatedSize += hash.getEstimatedSize();
            }
            if (groupDistinctRowStarts != null) {
                estimatedSize += sizeOf(groupDistinctRowStarts) + sizeOf(groupDistinctRowIds);
            }
            return estimatedSize;
        }

        @Override
        public void setGroupCount(int groupCount)
        {
            this.groupCount = max(this.groupCount, groupCount);
            accumulator.setGroupCount(groupCount);
        }

        @Override
        public void addInput(int[] groupIds, Page page, AggregationMask mask)
        {
            checkState(hash != null, "prepareFinal() has already been called");
            checkState(groupDistinctRowStarts == null, "evaluateIntermediate() has already been called");
            // 1. filter out positions based on mask
            groupIds = maskGroupIds(groupIds, mask);
            page = mask.filterPage(page);

            // 2. compute a mask for the distinct rows (including the group id)
            Work<Block> work = hash.markDistinctRows(page.prependColumn(new IntArrayBlock(page.getPositionCount(), Optional.empty(), groupIds)));
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. update original mask to the new distinct mask block
            mask.reset(page.getPositionCount());
            mask.applyMaskBlock(distinctMask);
            if (mask.isSelectNone()) {
                return;
            }

            // 4. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(groupIds, page, mask);
        }

        private static int[] maskGroupIds(int[] groupIds, AggregationMask mask)
        {
            if (mask.isSelectAll() || mask.isSelectNone()) {
                return groupIds;
            }

            int[] newGroupIds = new int[mask.getSelectedPositionCount()];
            int[] selectedPositions = mask.getSelectedPositions();
            for (int i = 0; i < newGroupIds.length; i++) {
                newGroupIds[i] = groupIds[selectedPositions[i]];
            }
            return newGroupIds;
        }

        @Override
        public void addIntermediate(int[] groupIds, Block block)
        {
            Rows rows = readRows(groupIds, block);
            addInput(rows.groupIds(), rows.page(), AggregationMask.createSelectAll(rows.page().getPositionCount()));
        }

        // Called for spilling, in any group order
        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            checkState(hash != null, "prepareFinal() has already been called");
            checkArgument(groupId < groupCount, "groupId %s is not below group count %s", groupId, groupCount);
            groupDistinctRows();
            int start = groupDistinctRowStarts[groupId];
            int end = groupDistinctRowStarts[groupId + 1];
            PageBuilder pageBuilder = spillPageBuilder();
            for (int index = start; index < end; index++) {
                hash.appendValuesTo(groupDistinctRowIds[index], pageBuilder);
                pageBuilder.declarePosition();
            }
            Page values = pageBuilder.build();
            writeRows(output, values.getPositionCount(), (row, fieldBuilders) -> {
                for (int field = 0; field < fieldBuilders.size(); field++) {
                    // channel 0 of the hash is the group id
                    Block block = values.getBlock(field + 1);
                    fieldBuilders.get(field).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(row));
                }
            });
        }

        private PageBuilder spillPageBuilder()
        {
            if (spillPageBuilder == null) {
                spillPageBuilder = new PageBuilder(hashTypes);
            }
            spillPageBuilder.reset();
            return spillPageBuilder;
        }

        // Orders the distinct ids by group, reading the group id of each distinct row from channel 0 of the hash
        private void groupDistinctRows()
        {
            if (groupDistinctRowStarts != null) {
                return;
            }
            int distinctRowCount = toIntExact(hash.getGroupCount());
            int[] groupIds = new int[distinctRowCount];
            int distinctId = 0;
            while (distinctId < distinctRowCount) {
                PageBuilder pageBuilder = spillPageBuilder();
                int firstDistinctId = distinctId;
                while (distinctId < distinctRowCount && !pageBuilder.isFull()) {
                    hash.appendValuesTo(distinctId, pageBuilder);
                    pageBuilder.declarePosition();
                    distinctId++;
                }
                Block groupIdBlock = pageBuilder.build().getBlock(0);
                for (int position = 0; position < groupIdBlock.getPositionCount(); position++) {
                    groupIds[firstDistinctId + position] = INTEGER.getInt(groupIdBlock, position);
                }
            }
            groupDistinctRowStarts = new int[groupCount + 1];
            for (distinctId = 0; distinctId < distinctRowCount; distinctId++) {
                groupDistinctRowStarts[groupIds[distinctId] + 1]++;
            }
            for (int groupId = 0; groupId < groupCount; groupId++) {
                groupDistinctRowStarts[groupId + 1] += groupDistinctRowStarts[groupId];
            }
            int[] nextPositions = Arrays.copyOf(groupDistinctRowStarts, groupCount);
            groupDistinctRowIds = new int[distinctRowCount];
            for (distinctId = 0; distinctId < distinctRowCount; distinctId++) {
                groupDistinctRowIds[nextPositions[groupIds[distinctId]]++] = distinctId;
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal(UpdateMemory updateMemory)
        {
            // release hash memory after all inputs have been added
            hash = null;
            accumulator.prepareFinal(updateMemory);
        }
    }
}
