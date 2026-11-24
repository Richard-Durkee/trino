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

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.operator.WorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * Marks distinct rows of an input that is grouped on all the distinct channels, so that equal rows are adjacent:
 * a row is distinct if and only if it differs from the row before it. Compared to {@link MarkDistinctOperator},
 * nothing but the last row is remembered, instead of all the distinct rows.
 * <p>
 * The output is the input with the marker appended as the last channel, as for {@link MarkDistinctOperator}.
 */
public class StreamingMarkDistinctOperator
        implements WorkProcessorOperator
{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<Type> sourceTypes,
            List<Integer> markDistinctChannels,
            JoinCompiler joinCompiler)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, sourceTypes, markDistinctChannels, joinCompiler));
    }

    private static class Factory
            implements WorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> markDistinctChannels;
        private final JoinCompiler joinCompiler;
        private boolean closed;

        private Factory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes, List<Integer> markDistinctChannels, JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.markDistinctChannels = ImmutableList.copyOf(requireNonNull(markDistinctChannels, "markDistinctChannels is null"));
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return StreamingMarkDistinctOperator.class.getSimpleName();
        }

        @Override
        public WorkProcessorOperator create(OperatorContext operatorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new StreamingMarkDistinctOperator(operatorContext, sourcePages, sourceTypes, markDistinctChannels, joinCompiler);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, sourceTypes, markDistinctChannels, joinCompiler);
        }
    }

    private final StreamingMarkDistinct markDistinct;
    private final WorkProcessor<Page> pages;

    private StreamingMarkDistinctOperator(
            OperatorContext operatorContext,
            WorkProcessor<Page> sourcePages,
            List<Type> sourceTypes,
            List<Integer> markDistinctChannels,
            JoinCompiler joinCompiler)
    {
        markDistinct = new StreamingMarkDistinct(operatorContext, sourceTypes, markDistinctChannels, joinCompiler);
        pages = sourcePages.transform(markDistinct);
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
    {
        // the operator can be closed without being finished, for example if the query fails or is cancelled
        markDistinct.close();
    }

    private static class StreamingMarkDistinct
            implements Transformation<Page, Page>, AutoCloseable
    {
        private final LocalMemoryContext userMemoryContext;
        private final int[] markDistinctChannels;
        private final PagesHashStrategy rowStrategy;

        // The distinct channels of the last row of the previous page, to tell whether the first row of the next
        // page repeats it. It is a copy of the row, so that the whole page is not retained by it.
        @Nullable
        private Page previousRow;

        private StreamingMarkDistinct(OperatorContext operatorContext, List<Type> sourceTypes, List<Integer> markDistinctChannels, JoinCompiler joinCompiler)
        {
            this.userMemoryContext = operatorContext.newLocalUserMemoryContext(StreamingMarkDistinctOperator.class.getSimpleName());
            this.markDistinctChannels = markDistinctChannels.stream().mapToInt(Integer::intValue).toArray();
            this.rowStrategy = joinCompiler.compilePagesHashStrategyFactory(sourceTypes, markDistinctChannels, Optional.empty())
                    .createPagesHashStrategy(sourceTypes.stream()
                            .map(_ -> new ObjectArrayList<Block>())
                            .collect(toImmutableList()));
        }

        @Override
        public TransformationState<Page> process(@Nullable Page inputPage)
        {
            if (inputPage == null) {
                close();
                return finished();
            }
            // pages that reach the operator are not empty, see PageBuffer
            int positionCount = inputPage.getPositionCount();

            Page distinctPage = inputPage.getColumns(markDistinctChannels);
            BlockBuilder markers = BOOLEAN.createFixedSizeBlockBuilder(positionCount);
            BOOLEAN.writeBoolean(markers, previousRow == null || !rowStrategy.rowIdenticalToRow(0, distinctPage, 0, previousRow));
            for (int position = 1; position < positionCount; position++) {
                BOOLEAN.writeBoolean(markers, !rowStrategy.rowIdenticalToRow(position - 1, distinctPage, position, distinctPage));
            }

            previousRow = distinctPage.copyPositions(new int[] {positionCount - 1}, 0, 1);
            userMemoryContext.setBytes(previousRow.getRetainedSizeInBytes());
            return ofResult(inputPage.appendColumn(markers.build()));
        }

        @Override
        public void close()
        {
            previousRow = null;
            userMemoryContext.close();
        }
    }
}
