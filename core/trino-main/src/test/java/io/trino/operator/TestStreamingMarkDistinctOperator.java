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
import io.trino.RowPagesBuilder;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStreamingMarkDistinctOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final TypeOperators typeOperators = new TypeOperators();
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(typeOperators, new NullSafeHashCompiler(typeOperators));
    private final JoinCompiler joinCompiler = new JoinCompiler(typeOperators);

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testGroupBoundaryInsideAndAtTheEndOfPages()
    {
        List<Type> types = ImmutableList.of(BIGINT);
        // runs of 1, 3, 2 and 4 equal values over pages of 4 rows: boundaries fall inside a page, and a run ends exactly at the end of a page
        List<Page> pages = rowPagesBuilder(types)
                .row(1L).row(2L).row(2L).row(2L)
                .pageBreak()
                .row(3L).row(3L).row(4L).row(4L)
                .pageBreak()
                .row(4L).row(4L).row(5L).row(5L)
                .build();
        assertMarkers(types, ImmutableList.of(0), pages, true, true, false, false, true, false, true, false, false, false, true, false);
    }

    @Test
    public void testRunsSpanningManyPages()
    {
        List<Type> types = ImmutableList.of(BIGINT);
        RowPagesBuilder builder = rowPagesBuilder(types);
        for (long value = 0; value < 3; value++) {
            for (int page = 0; page < 10; page++) {
                builder.row(value).row(value);
                builder.pageBreak();
            }
        }
        List<Boolean> expected = new ArrayList<>();
        for (int value = 0; value < 3; value++) {
            expected.add(true);
            for (int i = 1; i < 20; i++) {
                expected.add(false);
            }
        }
        assertThat(markers(types, ImmutableList.of(0), builder.build(), true)).isEqualTo(expected);
    }

    @Test
    public void testEmptyPagesInTheMiddle()
    {
        List<Type> types = ImmutableList.of(BIGINT);
        Page first = rowPagesBuilder(types).row(1L).row(2L).buildPage();
        Page second = rowPagesBuilder(types).row(2L).row(3L).buildPage();
        Page empty = second.getRegion(0, 0);
        // the empty pages neither end the run of 2 nor start another
        assertMarkers(types, ImmutableList.of(0), ImmutableList.of(first, empty, empty, second), true, true, false, true);
        assertMarkers(types, ImmutableList.of(0), ImmutableList.of(empty, first, empty), true, true);
    }

    @Test
    public void testNulls()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        List<Page> pages = rowPagesBuilder(types)
                .row(null, "a").row(null, "a")
                .pageBreak()
                .row(null, "a").row(null, null)
                .pageBreak()
                .row(null, null).row(1L, null)
                .build();
        // nulls are equal to each other, within and across pages
        assertMarkers(types, ImmutableList.of(0, 1), pages, true, false, false, true, false, true);
    }

    @Test
    public void testFloatingPoint()
    {
        List<Type> types = ImmutableList.of(DOUBLE);
        List<Page> pages = rowPagesBuilder(types)
                .row(0.0).row(-0.0).row(Double.NaN)
                .pageBreak()
                .row(Double.longBitsToDouble(0x7FF8000000000001L)).row(1.5)
                .build();
        // as in grouping: 0.0 is equal to -0.0, and all NaNs are equal
        assertMarkers(types, ImmutableList.of(0), pages, true, false, true, false, true);
    }

    @Test
    public void testSingleHugeGroup()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        RowPagesBuilder builder = rowPagesBuilder(types);
        for (int i = 0; i < 100_000; i++) {
            builder.row(7L, "same");
            if (i % 1000 == 999) {
                builder.pageBreak();
            }
        }
        List<Boolean> markers = markers(types, ImmutableList.of(0, 1), builder.build(), true);
        assertThat(markers).hasSize(100_000);
        assertThat(markers.stream().filter(marker -> marker).count()).isEqualTo(1);
        assertThat(markers.getFirst()).isTrue();
    }

    @Test
    public void testAllChannelsDefineTheRun()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        // only the second channel changes at the first boundary, and only the first channel at the second
        List<Page> pages = rowPagesBuilder(types)
                .row(1L, "a").row(1L, "b").row(1L, "b").row(2L, "b")
                .pageBreak()
                .row(3L, "b").row(3L, "b")
                .build();
        assertMarkers(types, ImmutableList.of(0, 1), pages, true, true, false, true, true, false);
    }

    @Test
    public void testDistinctChannelsAreNotLeadingAndNotInPageOrder()
    {
        // channel 0 is a payload that differs in every row, channels 1 and 2 are the distinct channels
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BIGINT);
        List<Page> pages = rowPagesBuilder(types)
                .row(10L, "x", 1L).row(11L, "x", 1L).row(12L, "x", 2L)
                .pageBreak()
                .row(13L, "y", 2L).row(14L, "y", 2L)
                .build();
        assertMarkers(types, ImmutableList.of(2, 1), pages, true, false, true, true, false);
        assertMarkers(types, ImmutableList.of(1, 2), pages, true, false, true, true, false);
        assertMarkers(types, ImmutableList.of(2), pages, true, false, true, false, false);
        assertMarkers(types, ImmutableList.of(1), pages, true, false, false, true, false);
    }

    @Test
    public void testDictionaryAndRunLengthEncodedBlocks()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        // equal values behind different dictionary ids are equal rows
        Block dictionary = createStringsBlock("a", "a", "b");
        Block ids = DictionaryBlock.create(6, dictionary, new int[] {0, 1, 1, 2, 2, 2});
        Block constant = RunLengthEncodedBlock.create(createLongsBlock(5L), 6);
        Page first = new Page(ids, constant);
        Page second = new Page(
                RunLengthEncodedBlock.create(createStringsBlock("c"), 2),
                DictionaryBlock.create(2, createLongsBlock(5L, 6L), new int[] {0, 1}));
        assertMarkers(types, ImmutableList.of(0, 1), ImmutableList.of(first, second), true, false, false, true, false, false, true, true);
    }

    @Test
    public void testComplexTypes()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        RowType rowType = RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR));
        List<Type> types = ImmutableList.of(arrayType, rowType);
        Block arrays = buildArrays(arrayType, ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L), ImmutableList.of(1L), ImmutableList.of(1L));
        Block rows = createRowBlock(ImmutableList.of(BIGINT, VARCHAR), new Object[] {1L, "a"}, new Object[] {1L, "a"}, new Object[] {1L, "a"}, new Object[] {1L, "b"});
        Page page = new Page(arrays, rows);
        // the arrays are equal in the first two rows, the rows in the first three, and both only in the first two together
        assertMarkers(types, ImmutableList.of(0, 1), ImmutableList.of(page), true, false, true, true);
        assertMarkers(types, ImmutableList.of(0), ImmutableList.of(page), true, false, true, false);
        assertMarkers(types, ImmutableList.of(1), ImmutableList.of(page), true, false, false, true);
    }

    @Test
    public void testFinishWithCarriedRow()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        Page page = rowPagesBuilder(types).row(1L).row(1L).buildPage();
        OperatorFactory factory = StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), types, ImmutableList.of(0), joinCompiler);
        DriverContext driverContext = newDriverContext();
        try (Operator operator = factory.createOperator(driverContext)) {
            operator.addInput(page);
            Page output = operator.getOutput();
            assertThat(output).isNotNull();
            assertThat(output.getChannelCount()).isEqualTo(2);
            // one row is kept to compare the next page with
            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0).isLessThan(1_000);
            operator.finish();
            assertThat(operator.getOutput()).isNull();
            assertThat(operator.isFinished()).isTrue();
            assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
        }
    }

    @Test
    public void testMemoryDoesNotGrowWithTheGroupSize()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        DriverContext driverContext = newDriverContext();
        OperatorFactory factory = StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), types, ImmutableList.of(0, 1), joinCompiler);
        try (Operator operator = factory.createOperator(driverContext)) {
            for (int i = 0; i < 100; i++) {
                RowPagesBuilder builder = rowPagesBuilder(types);
                for (int row = 0; row < 1_000; row++) {
                    builder.row((long) i * 1_000 + row, "a-rather-long-value-to-make-pages-big-" + row);
                }
                operator.addInput(builder.build().getFirst());
                assertThat(operator.getOutput()).isNotNull();
                assertThat(driverContext.getMemoryUsage()).isLessThan(2_000);
            }
        }
    }

    @Test
    public void testOperatorTypeAndInputProtocol()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        OperatorFactory factory = StreamingMarkDistinctOperator.createOperatorFactory(3, new PlanNodeId("node"), types, ImmutableList.of(0), joinCompiler);
        try (Operator operator = factory.createOperator(newDriverContext())) {
            assertThat(operator.getOperatorContext().getOperatorType()).isEqualTo("StreamingMarkDistinctOperator");
            assertThat(operator.getOperatorContext().getOperatorId()).isEqualTo(3);
            // the operator is never blocked, and takes one page at a time
            assertThat(operator.isBlocked().isDone()).isTrue();
            assertThat(operator.needsInput()).isTrue();
            operator.addInput(rowPagesBuilder(types).row(1L).buildPage());
            assertThat(operator.needsInput()).isFalse();
            assertThat(operator.getOutput()).isNotNull();
            assertThat(operator.needsInput()).isTrue();
            assertThat(operator.isFinished()).isFalse();
        }
    }

    @Test
    public void testDuplicatedFactoryHasItsOwnState()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        OperatorFactory factory = StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), types, ImmutableList.of(0), joinCompiler);
        OperatorFactory duplicate = factory.duplicate();
        // closing one factory does not close the other
        factory.noMoreOperators();
        assertThatThrownBy(() -> factory.createOperator(newDriverContext()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        // and operators of the duplicate do not share the previous row with the other one
        Page page = rowPagesBuilder(types).row(1L).buildPage();
        try (Operator first = duplicate.createOperator(newDriverContext());
                Operator second = duplicate.createOperator(newDriverContext())) {
            first.addInput(page);
            assertThat(BOOLEAN.getBoolean(first.getOutput().getBlock(1), 0)).isTrue();
            second.addInput(page);
            assertThat(BOOLEAN.getBoolean(second.getOutput().getBlock(1), 0)).isTrue();
        }
        duplicate.noMoreOperators();
        assertThatThrownBy(() -> duplicate.createOperator(newDriverContext()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testMemoryIsReleasedWhenClosedBeforeFinish()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        DriverContext driverContext = newDriverContext();
        OperatorFactory factory = StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), types, ImmutableList.of(0, 1), joinCompiler);
        Operator operator = factory.createOperator(driverContext);
        operator.addInput(rowPagesBuilder(types).row(1L, "a").buildPage());
        assertThat(operator.getOutput()).isNotNull();
        assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);
        // as when the query fails or is cancelled
        operator.close();
        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);
    }

    @Test
    public void testFactoryRequiresDistinctChannels()
    {
        assertThatThrownBy(() -> StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(BIGINT), ImmutableList.of(), joinCompiler))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSameOutputAsMarkDistinctOnRandomGroupedInput()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        for (int seed = 0; seed < 50; seed++) {
            Random random = new Random(seed);
            // the group (channel 0, with nulls) and the key (channel 1) are the distinct channels; the input is grouped on them
            // when a run of equal rows is never split by another row
            List<Object[]> rows = new ArrayList<>();
            int groupCount = 1 + random.nextInt(60);
            for (int group = 0; group < groupCount; group++) {
                Object groupValue = group % 7 == 0 ? null : "g" + group;
                // a null key only with a group value, so that no two groups are equal
                Object key = groupValue != null && random.nextInt(5) == 0 ? null : (long) group;
                int size = 1 + random.nextInt(25);
                for (int i = 0; i < size; i++) {
                    rows.add(new Object[] {groupValue, key, (long) rows.size()});
                }
            }
            int pageSize = 1 + random.nextInt(40);
            RowPagesBuilder builder = rowPagesBuilder(types);
            for (int i = 0; i < rows.size(); i++) {
                builder.row(rows.get(i));
                if ((i + 1) % pageSize == 0 || random.nextInt(30) == 0) {
                    builder.pageBreak();
                }
            }
            List<Page> pages = builder.build();
            // payload and marker, in order
            assertThat(run(types, ImmutableList.of(0, 1), pages, true))
                    .describedAs("seed %s", seed)
                    .isEqualTo(run(types, ImmutableList.of(0, 1), pages, false));
        }
    }

    private void assertMarkers(List<Type> types, List<Integer> distinctChannels, List<Page> pages, boolean... expected)
    {
        List<Boolean> expectedMarkers = new ArrayList<>();
        for (boolean marker : expected) {
            expectedMarkers.add(marker);
        }
        assertThat(markers(types, distinctChannels, pages, true)).isEqualTo(expectedMarkers);
        // and MarkDistinctOperator agrees, as these inputs are grouped
        assertThat(markers(types, distinctChannels, pages, false)).isEqualTo(expectedMarkers);
        assertThat(run(types, distinctChannels, pages, true)).isEqualTo(run(types, distinctChannels, pages, false));
    }

    private List<Boolean> markers(List<Type> types, List<Integer> distinctChannels, List<Page> pages, boolean streaming)
    {
        List<Boolean> markers = new ArrayList<>();
        for (MaterializedRow row : run(types, distinctChannels, pages, streaming)) {
            markers.add((Boolean) row.getField(types.size()));
        }
        return markers;
    }

    private List<MaterializedRow> run(List<Type> types, List<Integer> distinctChannels, List<Page> pages, boolean streaming)
    {
        OperatorFactory factory;
        if (streaming) {
            factory = StreamingMarkDistinctOperator.createOperatorFactory(0, new PlanNodeId("test"), types, distinctChannels, joinCompiler);
        }
        else {
            factory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), types, distinctChannels, hashStrategyCompiler);
        }
        List<Page> output = toPages(factory, newDriverContext(), pages);
        return toMaterializedResult(ImmutableList.<Type>builder().addAll(types).add(BOOLEAN).build(), output).getMaterializedRows();
    }

    @SafeVarargs
    private static Block buildArrays(ArrayType type, List<Long>... arrays)
    {
        var builder = type.createBlockBuilder(null, arrays.length);
        for (List<Long> array : arrays) {
            type.writeObject(builder, createLongsBlock(array.toArray(new Long[0])));
        }
        return builder.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
