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
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestSpillableMarkDistinctHash
{
    private static final DataSize LARGE = DataSize.of(64, DataSize.Unit.MEGABYTE);

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
    public void testRepeatedRevokesDownToZeroHotPartitions()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = randomLongPages(40, 1_000, 5_000, 1);

        List<Page> control = runWithoutSpill(types, new int[] {1}, pages);
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
            List<Page> output = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) {
                addAndCollect(hash, pages.get(i), output);
                if (i % 4 == 3) {
                    getFutureValue(hash.startMemoryRevoke());
                    hash.finishMemoryRevoke();
                }
            }
            assertThat(hash.hotPartitionCount()).isEqualTo(0);
            hash.startDrain();
            drain(hash, output);

            assertOneDistinctMarkerPerKey(types, pages, output, 1);
            // rows seen before the first revoke are answered immediately, exactly as without spilling
            Map<Long, Boolean> expected = markersById(control);
            Map<Long, Boolean> actual = markersById(output);
            for (Page page : pages.subList(0, 4)) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    long id = BIGINT.getLong(page.getBlock(0), position);
                    assertThat(actual.get(id)).describedAs("marker of row %s", id).isEqualTo(expected.get(id));
                }
            }
            assertThat(spillerFactory.spillCalls()).isGreaterThan(0);
        }
    }

    @Test
    public void testRevokeWhileWorkIsYielding()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Object[]> first = new ArrayList<>();
        for (long i = 0; i < 50_000; i++) {
            first.add(new Object[] {i, i});
        }
        List<Object[]> second = new ArrayList<>();
        for (long i = 0; i < 50_000; i++) {
            second.add(new Object[] {50_000 + i, i});
        }
        List<Page> pages = ImmutableList.of(page(types, first), page(types, second));

        AtomicReference<SpillableMarkDistinctHash> hashReference = new AtomicReference<>();
        AtomicBoolean memoryAvailable = new AtomicBoolean();
        UpdateMemory updateMemory = () -> memoryAvailable.get() || hashReference.get() == null || hashReference.get().getEstimatedSize() < 200_000;
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, updateMemory, spillerFactory, LARGE)) {
            hashReference.set(hash);
            List<Page> output = new ArrayList<>();

            Work<Page> work = hash.markDistinctRows(pages.get(0));
            assertThat(work.process()).describedAs("work is expected to yield on memory").isFalse();
            // the revoke arrives while the work is unfinished
            getFutureValue(hash.startMemoryRevoke());
            hash.finishMemoryRevoke();
            memoryAvailable.set(true);
            assertThat(work.process()).isTrue();
            addResult(work.getResult(), output);

            addAndCollect(hash, pages.get(1), output);
            hash.startDrain();
            drain(hash, output);

            assertOneDistinctMarkerPerKey(types, pages, output, 1);
            assertThat(markersById(output)).hasSize(100_000);
        }
    }

    @Test
    public void testRevokeWithNothingResidentDoesNotSwitchToSpilledMode()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
            getFutureValue(hash.startMemoryRevoke());
            assertThat(hash.isSpilled()).isFalse();
            assertThat(spillerFactory.spillCalls()).isEqualTo(0);
        }
    }

    @Test
    public void testRevokeAfterDrainStartedIsIgnored()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = randomLongPages(8, 1_000, 2_000, 2);
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
            List<Page> output = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) {
                addAndCollect(hash, pages.get(i), output);
                if (i == 3) {
                    getFutureValue(hash.startMemoryRevoke());
                }
            }
            hash.startDrain();
            assertThat(hash.getEstimatedSize()).describedAs("resident hash is released at drain").isEqualTo(0);
            getFutureValue(hash.startMemoryRevoke());
            hash.finishMemoryRevoke();
            drain(hash, output);
            // revoking again after everything was drained must not fail either
            getFutureValue(hash.startMemoryRevoke());
            assertOneDistinctMarkerPerKey(types, pages, output, 1);
        }
    }

    @Test
    public void testNullsAndMultipleDistinctChannels()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, VARCHAR, BIGINT);
        Random random = new Random(3);
        List<Page> pages = new ArrayList<>();
        for (int p = 0; p < 20; p++) {
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Object a = random.nextInt(10) == 0 ? null : (long) random.nextInt(30);
                Object b = random.nextInt(10) == 0 ? null : "v" + random.nextInt(20);
                rows.add(new Object[] {(long) p * 500 + i, a, b, (long) i});
            }
            pages.add(page(types, rows));
        }
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1, 2}, () -> true, spillerFactory, LARGE)) {
            List<Page> output = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) {
                addAndCollect(hash, pages.get(i), output);
                if (i % 3 == 2) {
                    getFutureValue(hash.startMemoryRevoke());
                }
            }
            hash.startDrain();
            drain(hash, output);
            assertOneDistinctMarkerPerKey(types, pages, output, 1, 2);
            assertThat(markersById(output)).hasSize(20 * 500);
            // the payload column must survive the round trip through the spill files
            for (Page page : output) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    long rowId = BIGINT.getLong(page.getBlock(0), position);
                    assertThat(BIGINT.getLong(page.getBlock(3), position)).isEqualTo(rowId % 500);
                }
            }
        }
    }

    @Test
    public void testSingleHotKeyWithManyDuplicatesIsNotBuffered()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, VARCHAR);
        String payload = "x".repeat(200);
        long rowsPerPage = 2_000;
        int pageCount = 50;
        List<Page> pages = new ArrayList<>();
        for (int p = 0; p < pageCount; p++) {
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < rowsPerPage; i++) {
                rows.add(new Object[] {p * rowsPerPage + i, 7L, payload});
            }
            pages.add(page(types, rows));
        }
        // the partition is far larger than the limit below, but it holds one key
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, DataSize.of(256, DataSize.Unit.KILOBYTE))) {
            List<Page> output = new ArrayList<>();
            for (Page page : pages) {
                addAndCollect(hash, page, output);
                getFutureValue(hash.startMemoryRevoke());
            }
            hash.startDrain();
            long peak = 0;
            Page page;
            while ((page = hash.pollNextDrainedPage()) != null) {
                output.add(page);
                peak = Math.max(peak, hash.getEstimatedSize());
            }
            assertOneDistinctMarkerPerKey(types, pages, output, 1);
            assertThat(markersById(output)).hasSize(pageCount * (int) rowsPerPage);
            // raw rows total about 20 MB, but only one key is ever held
            assertThat(peak).isLessThan(1L << 20);
        }
    }

    @Test
    public void testSkewedPartitionsAreSplitRecursively()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = randomLongPages(30, 2_000, 20_000, 4);
        // a one byte limit splits every partition at every level until the maximum depth is reached
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, DataSize.ofBytes(1))) {
            List<Page> output = new ArrayList<>();
            for (int i = 0; i < pages.size(); i++) {
                addAndCollect(hash, pages.get(i), output);
                if (i % 5 == 4) {
                    getFutureValue(hash.startMemoryRevoke());
                }
            }
            hash.startDrain();
            drain(hash, output);
            assertOneDistinctMarkerPerKey(types, pages, output, 1);
            assertThat(markersById(output)).hasSize(60_000);
        }
    }

    @Test
    public void testFloatingPointKeysThatAreEqualGroupTogether()
            throws Exception
    {
        double nanWithPayload = Double.longBitsToDouble(0x7FF8000000000001L);
        float realNanWithPayload = intBitsToFloat(0x7FC00001);
        List<Object[]> doubles = new ArrayList<>();
        List<Object[]> reals = new ArrayList<>();
        long id = 0;
        for (int i = 0; i < 200; i++) {
            Object[] doubleValues = {0.0, -0.0, Double.NaN, nanWithPayload, 1.5, null};
            Object[] realValues = {0.0f, -0.0f, Float.NaN, realNanWithPayload, 1.5f, null};
            for (int j = 0; j < doubleValues.length; j++) {
                doubles.add(new Object[] {id, doubleValues[j]});
                reals.add(new Object[] {id, realValues[j]});
                id++;
            }
        }
        for (boolean useDouble : new boolean[] {true, false}) {
            Type type = useDouble ? DOUBLE : REAL;
            List<Type> types = ImmutableList.of(BIGINT, type);
            List<Object[]> rows = useDouble ? doubles : reals;
            List<Page> pages = new ArrayList<>();
            for (int start = 0; start < rows.size(); start += 100) {
                pages.add(page(types, rows.subList(start, Math.min(rows.size(), start + 100))));
            }
            try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                    SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
                List<Page> output = new ArrayList<>();
                // revoke first, so that every row is routed by the partition hash
                getFutureValue(hash.startMemoryRevoke());
                addAndCollect(hash, pages.get(0), output);
                getFutureValue(hash.startMemoryRevoke());
                for (Page page : pages.subList(1, pages.size())) {
                    addAndCollect(hash, page, output);
                    getFutureValue(hash.startMemoryRevoke());
                }
                hash.startDrain();
                drain(hash, output);
                // classes are: zero (0.0 and -0.0), NaN, 1.5 and null
                assertOneDistinctMarkerPerKey(types, pages, output, 1);
                long trues = markersById(output).values().stream().filter(marker -> marker).count();
                assertThat(trues).isEqualTo(4);
            }
        }
    }

    @Test
    public void testSpillFilesAreReleasedAsPartitionsAreDrained()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = randomLongPages(20, 2_000, 10_000, 5);
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory()) {
            try (SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
                List<Page> output = new ArrayList<>();
                for (int i = 0; i < pages.size(); i++) {
                    addAndCollect(hash, pages.get(i), output);
                    if (i % 2 == 1) {
                        getFutureValue(hash.startMemoryRevoke());
                    }
                }
                hash.startDrain();
                long filesBeforeDrain = spillerFactory.liveSpillFiles();
                assertThat(filesBeforeDrain).isGreaterThan(0);
                long minimum = filesBeforeDrain;
                Page page;
                while ((page = hash.pollNextDrainedPage()) != null) {
                    output.add(page);
                    minimum = Math.min(minimum, spillerFactory.liveSpillFiles());
                }
                assertThat(minimum).describedAs("files of drained partitions are removed before close").isLessThan(filesBeforeDrain);
                assertThat(spillerFactory.liveSpillFiles()).isEqualTo(0);
                assertOneDistinctMarkerPerKey(types, pages, output, 1);
            }
            assertThat(spillerFactory.liveSpillFiles()).isEqualTo(0);
        }
    }

    @Test
    public void testSpillFilesAreReleasedOnCloseAfterFailure()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = randomLongPages(10, 2_000, 10_000, 6);
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory()) {
            assertThatThrownBy(() -> {
                try (SpillableMarkDistinctHash hash = newHash(types, new int[] {1}, () -> true, spillerFactory, LARGE)) {
                    List<Page> output = new ArrayList<>();
                    for (Page page : pages) {
                        addAndCollect(hash, page, output);
                        getFutureValue(hash.startMemoryRevoke());
                    }
                    hash.startDrain();
                    assertThat(hash.pollNextDrainedPage()).isNotNull();
                    assertThat(spillerFactory.liveSpillFiles()).isGreaterThan(0);
                    throw new IllegalStateException("simulated failure in the middle of the drain");
                }
            }).isInstanceOf(IllegalStateException.class);
            assertThat(spillerFactory.liveSpillFiles()).isEqualTo(0);
        }
    }

    private List<Page> runWithoutSpill(List<Type> types, int[] channels, List<Page> pages)
            throws Exception
    {
        try (TestingFileSpillerFactory spillerFactory = new TestingFileSpillerFactory();
                SpillableMarkDistinctHash hash = newHash(types, channels, () -> true, spillerFactory, LARGE)) {
            List<Page> output = new ArrayList<>();
            for (Page page : pages) {
                addAndCollect(hash, page, output);
            }
            hash.startDrain();
            drain(hash, output);
            assertThat(spillerFactory.spillCalls()).isEqualTo(0);
            return output;
        }
    }

    private SpillableMarkDistinctHash newHash(List<Type> types, int[] channels, UpdateMemory updateMemory, TestingFileSpillerFactory spillerFactory, DataSize maxResolveSize)
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "test");
        List<Type> distinctTypes = new ArrayList<>();
        for (int channel : channels) {
            distinctTypes.add(types.get(channel));
        }
        return new SpillableMarkDistinctHash(
                driverContext.getSession(),
                distinctTypes,
                types,
                channels,
                hashStrategyCompiler,
                updateMemory,
                true,
                spillerFactory,
                operatorContext,
                maxResolveSize);
    }

    private static void addAndCollect(SpillableMarkDistinctHash hash, Page page, List<Page> output)
    {
        Work<Page> work = hash.markDistinctRows(page);
        while (!work.process()) {
            // no yielding expected
        }
        addResult(work.getResult(), output);
    }

    private static void addResult(Page result, List<Page> output)
    {
        if (result != null) {
            output.add(result);
        }
    }

    private static void drain(SpillableMarkDistinctHash hash, List<Page> output)
    {
        Page page;
        while ((page = hash.pollNextDrainedPage()) != null) {
            output.add(page);
        }
    }

    private static Map<Long, Boolean> markersById(List<Page> output)
    {
        Map<Long, Boolean> markers = new HashMap<>();
        for (Page page : output) {
            Block marker = page.getBlock(page.getChannelCount() - 1);
            for (int position = 0; position < page.getPositionCount(); position++) {
                Boolean previous = markers.put(BIGINT.getLong(page.getBlock(0), position), BOOLEAN.getBoolean(marker, position));
                assertThat(previous).describedAs("row emitted twice").isNull();
            }
        }
        return markers;
    }

    /**
     * Every input row (identified by channel 0) is emitted exactly once, and for each distinct key (as
     * the key columns are grouped, so for example -0.0 and 0.0 are the same key) exactly one row is marked.
     */
    private static void assertOneDistinctMarkerPerKey(List<Type> types, List<Page> input, List<Page> output, int... keyChannels)
    {
        Map<Long, Boolean> markers = markersById(output);
        Map<List<Object>, Integer> trueCountPerKey = new HashMap<>();
        long rows = 0;
        for (Page page : input) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                rows++;
                long id = BIGINT.getLong(page.getBlock(0), position);
                assertThat(markers).describedAs("row %s was emitted", id).containsKey(id);
                List<Object> key = new ArrayList<>();
                for (int channel : keyChannels) {
                    key.add(normalizedValue(types.get(channel), page.getBlock(channel), position));
                }
                trueCountPerKey.merge(key, markers.get(id) ? 1 : 0, Integer::sum);
            }
        }
        assertThat(markers).hasSize((int) rows);
        trueCountPerKey.forEach((key, trues) -> assertThat(trues).describedAs("distinct markers for key %s", key).isEqualTo(1));
    }

    private static Object normalizedValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return "null";
        }
        if (type == BIGINT) {
            return BIGINT.getLong(block, position);
        }
        if (type == VARCHAR) {
            return VARCHAR.getSlice(block, position).toStringUtf8();
        }
        if (type == DOUBLE) {
            return normalizeFloating(DOUBLE.getDouble(block, position));
        }
        if (type == REAL) {
            return normalizeFloating(intBitsToFloat(REAL.getInt(block, position)));
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static Object normalizeFloating(double value)
    {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        return value == 0 ? 0.0 : value;
    }

    private static List<Page> randomLongPages(int pageCount, int rowsPerPage, int distinctKeys, long seed)
    {
        Random random = new Random(seed);
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<Page> pages = new ArrayList<>();
        for (int p = 0; p < pageCount; p++) {
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < rowsPerPage; i++) {
                rows.add(new Object[] {(long) p * rowsPerPage + i, (long) random.nextInt(distinctKeys)});
            }
            pages.add(page(types, rows));
        }
        return pages;
    }

    private static Page page(List<Type> types, List<Object[]> rows)
    {
        PageBuilder builder = new PageBuilder(types);
        for (Object[] row : rows) {
            builder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                Object value = row[channel];
                if (value == null) {
                    builder.getBlockBuilder(channel).appendNull();
                }
                else if (type == BIGINT) {
                    BIGINT.writeLong(builder.getBlockBuilder(channel), (Long) value);
                }
                else if (type == DOUBLE) {
                    DOUBLE.writeDouble(builder.getBlockBuilder(channel), (Double) value);
                }
                else if (type == REAL) {
                    REAL.writeInt(builder.getBlockBuilder(channel), floatToRawIntBits((Float) value));
                }
                else if (type == VARCHAR) {
                    VARCHAR.writeSlice(builder.getBlockBuilder(channel), Slices.utf8Slice((String) value));
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
        return builder.build();
    }
}
