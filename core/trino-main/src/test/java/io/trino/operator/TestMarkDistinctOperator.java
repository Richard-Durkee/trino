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
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.createPages;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMarkDistinctOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final TypeOperators typeOperators = new TypeOperators();
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(typeOperators, new NullSafeHashCompiler(typeOperators));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMarkDistinct()
    {
        DriverContext driverContext = newDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                ImmutableList.of(0),
                hashStrategyCompiler,
                false,
                new DummySpillerFactory());

        MaterializedResult.Builder expected = resultBuilder(driverContext.getSession(), BIGINT, BOOLEAN);
        for (long i = 0; i < 100; i++) {
            expected.row(i, true);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected.build());
    }

    @Test
    public void testRleDistinctMask()
    {
        DriverContext driverContext = newDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
        List<Page> inputs = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 50)
                .addSequencePage(1, 200)
                .addSequencePage(1, 100)
                .build();
        Page firstInput = inputs.get(0);
        Page secondInput = inputs.get(1);
        Page singleDistinctPage = inputs.get(2);
        Page singleNotDistinctPage = inputs.get(3);
        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                ImmutableList.of(0),
                hashStrategyCompiler,
                false,
                new DummySpillerFactory());

        int maskChannel = firstInput.getChannelCount(); // mask channel is appended to the input
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operator.addInput(firstInput);
            Block allDistinctOutput = operator.getOutput().getBlock(maskChannel);
            operator.addInput(firstInput);
            Block noDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // all distinct and no distinct conditions produce RLE blocks
            assertThat(allDistinctOutput).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(BOOLEAN.getBoolean(allDistinctOutput, 0)).isTrue();
            assertThat(noDistinctOutput).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(BOOLEAN.getBoolean(noDistinctOutput, 0)).isFalse();

            operator.addInput(secondInput);
            Block halfDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // [0,50) is not distinct
            for (int position = 0; position < 50; position++) {
                assertThat(BOOLEAN.getBoolean(halfDistinctOutput, position)).isFalse();
            }
            for (int position = 50; position < 100; position++) {
                assertThat(BOOLEAN.getBoolean(halfDistinctOutput, position)).isTrue();
            }

            operator.addInput(singleDistinctPage);
            Block singleDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertThat(singleDistinctBlock instanceof RunLengthEncodedBlock)
                    .describedAs("single position inputs should not be RLE")
                    .isFalse();
            assertThat(BOOLEAN.getBoolean(singleDistinctBlock, 0)).isTrue();

            operator.addInput(singleNotDistinctPage);
            Block singleNotDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertThat(singleNotDistinctBlock instanceof RunLengthEncodedBlock)
                    .describedAs("single position inputs should not be RLE")
                    .isFalse();
            assertThat(BOOLEAN.getBoolean(singleNotDistinctBlock, 0)).isFalse();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMarkDistinctSpill()
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();
        DriverContext driverContext = newDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(1_000, 0)
                .addSequencePage(1_000, 500)
                .addSequencePage(1_000, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                ImmutableList.of(0),
                hashStrategyCompiler,
                true,
                spillerFactory);

        // value v in [0, 500) occurs twice (pages 1 and 3): one true, one false
        // value v in [500, 1000) occurs three times (pages 1, 2 and 3): one true, two false
        // value v in [1000, 1500) occurs once (page 2 only): one true
        MaterializedResult.Builder expected = resultBuilder(driverContext.getSession(), BIGINT, BOOLEAN);
        for (long i = 0; i < 1_500; i++) {
            expected.row(i, true);
        }
        for (long i = 0; i < 500; i++) {
            expected.row(i, false);
        }
        for (long i = 500; i < 1_000; i++) {
            expected.row(i, false);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected.build(), true);
        assertThat(spillerFactory.getSpillsCount()).isGreaterThan(0);
    }

    @Test
    public void testSpillableMarkDistinctHashRecursivePartitioning()
            throws Exception
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();
        DriverContext driverContext = newDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "test");
        List<Type> distinctTypes = ImmutableList.of(BIGINT);

        // A tiny resolve-size threshold forces every spilled partition to be recursively re-partitioned
        // at least once before it can be resolved, exercising the SpillableMarkDistinctHash recursion path
        // directly (an operator-level test can't reliably force this: real spilled data sizes are too far
        // below any realistic production threshold to trip it deterministically).
        try (SpillableMarkDistinctHash hash = new SpillableMarkDistinctHash(
                driverContext.getSession(),
                distinctTypes,
                distinctTypes,
                new int[] {0},
                hashStrategyCompiler,
                () -> true,
                true,
                spillerFactory,
                operatorContext,
                DataSize.ofBytes(256))) {
            RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
            List<Page> input = rowPagesBuilder
                    .addSequencePage(2_000, 0)
                    .addSequencePage(2_000, 0)
                    .build();

            List<Page> output = new ArrayList<>();
            for (Page page : input) {
                Work<Page> work = hash.markDistinctRows(page);
                while (!work.process()) {
                    // no yielding expected: every page here is already fully buffered in memory
                }
                Page result = work.getResult();
                if (result != null) {
                    output.add(result);
                }
                getFutureValue(hash.startMemoryRevoke());
                hash.finishMemoryRevoke();
            }

            Page drained;
            while ((drained = hash.pollNextDrainedPage()) != null) {
                output.add(drained);
            }

            long trueCount = 0;
            long falseCount = 0;
            for (Page page : output) {
                Block marker = page.getBlock(1);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (BOOLEAN.getBoolean(marker, position)) {
                        trueCount++;
                    }
                    else {
                        falseCount++;
                    }
                }
            }
            assertThat(trueCount).isEqualTo(2_000);
            assertThat(falseCount).isEqualTo(2_000);
            assertThat(spillerFactory.getSpillsCount()).isGreaterThan(0);
        }
    }

    @Test
    public void testMemoryReservationYield()
            throws Exception
    {
        testMemoryReservationYield(BIGINT);
        testMemoryReservationYield(VARCHAR);
    }

    private void testMemoryReservationYield(Type type)
            throws Exception
    {
        List<Page> input = createPages(type, 6_000, 600);

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(type), ImmutableList.of(0), hashStrategyCompiler, false, new DummySpillerFactory());

        // get result with yield; pick a relatively small buffer for partitionRowCount's memory usage
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, operator -> ((MarkDistinctOperator) operator).getCapacity(), 450_000);
        assertThat(result.yieldCount()).isGreaterThanOrEqualTo(5);
        assertThat(result.maxReservedBytes()).isGreaterThanOrEqualTo(20L << 20);

        int count = 0;
        for (Page page : result.output()) {
            assertThat(page.getChannelCount()).isEqualTo(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(BOOLEAN.getBoolean(page.getBlock(1), i)).isTrue();
                count++;
            }
        }
        assertThat(count).isEqualTo(6_000 * 600);
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
