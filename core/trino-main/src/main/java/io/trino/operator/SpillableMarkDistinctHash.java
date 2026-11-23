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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Adds hash-partitioned ("grace hash" / hybrid hash) external spilling on top of {@link MarkDistinctHash}.
 * <p>
 * {@link MarkDistinctOperator} must remember every distinct key it has ever seen for the lifetime of the
 * operator. This class hash-partitions the key space into a fixed number of buckets. Because all
 * occurrences of a given key always land in the same bucket regardless of arrival time, a bucket that is
 * spilled to disk ("cold") can safely defer every one of its rows' distinct decisions to a later, one-shot
 * resolution pass once the input is finished, while buckets that remain resident ("hot") keep resolving
 * rows immediately. Row order across the operator's output is not preserved once anything has spilled.
 * <p>
 * Each cold partition is resolved by a nested instance of this class: the keys that were resolved before
 * the partition went cold are inserted first, then the spilled rows are streamed through. The nested
 * instance partitions itself again (with a different hash salt) if its resident keys grow beyond the
 * configured size, so memory is bounded by the number of distinct keys rather than the number of rows. A
 * partition made of one repeated key therefore stays small and is never buffered.
 */
public class SpillableMarkDistinctHash
        implements AutoCloseable
{
    @VisibleForTesting
    static final int DEFAULT_PARTITION_COUNT = 32;
    @VisibleForTesting
    static final int RECURSIVE_PARTITION_COUNT = 8;
    @VisibleForTesting
    static final int MAX_RECURSION_DEPTH = 4;
    // Pages destined for a cold partition are buffered up to this size before they are written as one spill file
    private static final long SPILL_BUFFER_BYTES = 1L << 20;

    private final Session session;
    private final List<Type> distinctTypes;
    private final List<Type> rawTypes;
    private final int[] markDistinctChannels;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final UpdateMemory updateMemory;
    private final boolean spillEnabled;
    private final SpillerFactory spillerFactory;
    private final OperatorContext operatorContext;
    private final long maxResolveBytes;
    private final int depth;

    private MarkDistinctHash hotHash;
    private boolean suppressMemoryUpdates;
    // incremented whenever the hot hash is replaced, so that in-flight work can detect that its routing is stale
    private int epoch;

    private LocalPartitionGenerator partitionFunction;
    private boolean[] cold;
    private PartitionSpill[] seeds;
    private PartitionSpill[] raws;
    private long bufferedBytes;

    private boolean draining;
    private int drainCursor;
    private int resolverPartition = -1;
    private PartitionResolver resolver;

    public SpillableMarkDistinctHash(
            Session session,
            List<Type> distinctTypes,
            List<Type> rawTypes,
            int[] markDistinctChannels,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory,
            boolean spillEnabled,
            SpillerFactory spillerFactory,
            OperatorContext operatorContext,
            DataSize maxResolveSize)
    {
        this(session,
                distinctTypes,
                rawTypes,
                markDistinctChannels,
                hashStrategyCompiler,
                updateMemory,
                spillEnabled,
                spillerFactory,
                operatorContext,
                maxResolveSize,
                0);
    }

    private SpillableMarkDistinctHash(
            Session session,
            List<Type> distinctTypes,
            List<Type> rawTypes,
            int[] markDistinctChannels,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory,
            boolean spillEnabled,
            SpillerFactory spillerFactory,
            OperatorContext operatorContext,
            DataSize maxResolveSize,
            int depth)
    {
        this.session = requireNonNull(session, "session is null");
        this.distinctTypes = ImmutableList.copyOf(requireNonNull(distinctTypes, "distinctTypes is null"));
        this.rawTypes = ImmutableList.copyOf(requireNonNull(rawTypes, "rawTypes is null"));
        this.markDistinctChannels = requireNonNull(markDistinctChannels, "markDistinctChannels is null").clone();
        this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.spillEnabled = spillEnabled;
        this.spillerFactory = spillerFactory;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.maxResolveBytes = maxResolveSize.toBytes();
        this.depth = depth;
        this.hotHash = newHotHash();
    }

    private MarkDistinctHash newHotHash()
    {
        return new MarkDistinctHash(session, distinctTypes, hashStrategyCompiler, this::updateHotMemory);
    }

    private boolean updateHotMemory()
    {
        return suppressMemoryUpdates || updateMemory.update();
    }

    /**
     * Memory held by this instance: the resident hash, rows buffered for spilling and, while draining,
     * the resolution of the current cold partition. Before draining starts all of it is revocable.
     */
    public long getEstimatedSize()
    {
        long size = bufferedBytes;
        if (hotHash != null) {
            size += hotHash.getEstimatedSize();
        }
        if (resolver != null) {
            size += resolver.estimatedSize();
        }
        return size;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return hotHash.getCapacity();
    }

    @VisibleForTesting
    boolean isSpilled()
    {
        return partitionFunction != null;
    }

    @VisibleForTesting
    int hotPartitionCount()
    {
        return cold == null ? -1 : countHotPartitions();
    }

    public boolean isDraining()
    {
        return draining;
    }

    public boolean hasSpilledPartitions()
    {
        if (cold == null) {
            return false;
        }
        for (boolean isCold : cold) {
            if (isCold) {
                return true;
            }
        }
        return false;
    }

    public Work<Page> markDistinctRows(Page fullPage)
    {
        checkState(!draining, "Cannot add input after draining has started");
        return new MarkDistinctWork(fullPage);
    }

    /**
     * Rows of cold partitions are copied to the spill buffers while the work is routed; the remaining rows
     * are looked up in the resident hash. If the hash is rebuilt in between (a memory revoke while this work
     * is yielding), the unfinished rows are routed again against the new partitioning. This is safe because
     * a rebuild only keeps keys whose marker was already produced, so the keys inserted by an unfinished
     * work are forgotten and inserted again.
     */
    private final class MarkDistinctWork
            implements Work<Page>
    {
        private Page rows;
        private int routedEpoch = -1;
        private Work<Block> hotWork;
        private Page result;
        private boolean finished;

        private MarkDistinctWork(Page rows)
        {
            this.rows = requireNonNull(rows, "rows is null");
        }

        @Override
        public boolean process()
        {
            if (finished) {
                return true;
            }
            if (routedEpoch != epoch) {
                route();
                if (rows == null) {
                    finished = true;
                    return true;
                }
            }
            if (!hotWork.process()) {
                return false;
            }
            result = rows.appendColumn(hotWork.getResult());
            finished = true;
            return true;
        }

        private void route()
        {
            routedEpoch = epoch;
            if (partitionFunction != null) {
                rows = routeColdRows(rows, rows.getColumns(markDistinctChannels), raws);
                if (rows == null) {
                    return;
                }
            }
            hotWork = hotHash.markDistinctRows(rows.getColumns(markDistinctChannels));
        }

        @Override
        public Page getResult()
        {
            return result;
        }
    }

    public ListenableFuture<Void> startMemoryRevoke()
    {
        if (!spillEnabled || draining || hotHash == null) {
            return immediateVoidFuture();
        }
        if (hotHash.getGroupCount() == 0 && bufferedBytes == 0) {
            // nothing to free
            return immediateVoidFuture();
        }
        if (partitionFunction == null) {
            initializePartitioning(depth == 0 ? DEFAULT_PARTITION_COUNT : RECURSIVE_PARTITION_COUNT);
        }
        else {
            int hotPartitions = countHotPartitions();
            if (hotPartitions > 0) {
                demotePartitions(max(1, hotPartitions / 2));
            }
            else {
                flushAll();
            }
        }
        return immediateVoidFuture();
    }

    public void finishMemoryRevoke() {}

    /**
     * Called once the input is finished and no work is pending. Flushes all buffered rows, releases the
     * resident hash and makes the spilled partitions available through {@link #pollNextDrainedPage()}.
     */
    public void startDrain()
    {
        if (draining) {
            return;
        }
        flushAll();
        hotHash = null;
        draining = true;
    }

    /**
     * Adds already distinct keys, without producing markers. Used to seed a nested resolution.
     */
    private void seed(Page keys)
    {
        Page hotKeys = keys;
        if (partitionFunction != null) {
            hotKeys = routeColdRows(keys, keys, seeds);
        }
        if (hotKeys != null) {
            forceProcess(hotHash.markDistinctRows(hotKeys));
        }
    }

    private Page resolve(Page rawPage)
    {
        return forceProcess(markDistinctRows(rawPage));
    }

    private void splitIfNeeded()
    {
        if (depth < MAX_RECURSION_DEPTH && getEstimatedSize() > maxResolveBytes) {
            startMemoryRevoke();
        }
    }

    private void initializePartitioning(int partitionCount)
    {
        partitionFunction = new LocalPartitionGenerator(saltedHashGenerator(hashStrategyCompiler.getInterpretedHashGenerator(distinctTypes), depth), partitionCount);
        cold = new boolean[partitionCount];
        seeds = new PartitionSpill[partitionCount];
        raws = new PartitionSpill[partitionCount];
        demotePartitions(partitionCount / 2);
    }

    private int countHotPartitions()
    {
        int hot = 0;
        for (boolean isCold : cold) {
            if (!isCold) {
                hot++;
            }
        }
        return hot;
    }

    /**
     * Marks {@code count} hot partitions as cold (all of them if fewer are left), writes their resident keys
     * to the seed spill of the partition and rebuilds the resident hash from the keys of the partitions that
     * remain hot. The resident keys are visited in pages of bounded size. The cost is linear in the number of
     * resident keys and, for the duration of the rebuild, both the old and the rebuilt hash exist.
     */
    private void demotePartitions(int count)
    {
        List<Integer> hotPartitions = new ArrayList<>();
        for (int partition = 0; partition < cold.length; partition++) {
            if (!cold[partition]) {
                hotPartitions.add(partition);
            }
        }
        int toDemote = min(count, hotPartitions.size());
        for (int i = hotPartitions.size() - toDemote; i < hotPartitions.size(); i++) {
            cold[hotPartitions.get(i)] = true;
        }

        suppressMemoryUpdates = true;
        try {
            MarkDistinctHash rebuilt = newHotHash();
            hotHash.dumpMarkedGroups(distinctTypes, keys -> {
                Page hotKeys = routeColdRows(keys, keys, seeds);
                if (hotKeys != null) {
                    forceProcess(rebuilt.markDistinctRows(hotKeys));
                }
            });
            flushAll();
            hotHash = rebuilt;
            epoch++;
        }
        finally {
            suppressMemoryUpdates = false;
        }
    }

    /**
     * Copies the rows of {@code page} that belong to cold partitions into the buffers of {@code target} and
     * returns the remaining (hot) rows: the same page if all rows are hot, or null if none are.
     */
    private Page routeColdRows(Page page, Page distinctColumns, PartitionSpill[] target)
    {
        int positionCount = page.getPositionCount();
        int[] partitions = new int[positionCount];
        partitionFunction.getPartitions(distinctColumns, 0, positionCount, partitions);

        int[] hotPositions = new int[positionCount];
        int hotCount = 0;
        IntArrayList[] coldPositions = new IntArrayList[cold.length];
        for (int position = 0; position < positionCount; position++) {
            int partition = partitions[position];
            if (cold[partition]) {
                if (coldPositions[partition] == null) {
                    coldPositions[partition] = new IntArrayList();
                }
                coldPositions[partition].add(position);
            }
            else {
                hotPositions[hotCount++] = position;
            }
        }
        if (hotCount == positionCount) {
            return page;
        }
        for (int partition = 0; partition < coldPositions.length; partition++) {
            IntArrayList positions = coldPositions[partition];
            if (positions != null) {
                if (target[partition] == null) {
                    target[partition] = new PartitionSpill(target == seeds ? distinctTypes : rawTypes);
                }
                // copy, so that the buffered rows do not retain the whole input page
                target[partition].add(page.copyPositions(positions.elements(), 0, positions.size()));
            }
        }
        if (hotCount == 0) {
            return null;
        }
        return page.getPositions(hotPositions, 0, hotCount);
    }

    private void flushAll()
    {
        if (cold == null) {
            return;
        }
        for (int partition = 0; partition < cold.length; partition++) {
            if (seeds[partition] != null) {
                seeds[partition].flush();
            }
            if (raws[partition] != null) {
                raws[partition].flush();
            }
        }
    }

    /**
     * Returns the next page of rows whose marker was deferred because they belong to a spilled partition,
     * or null when all of them have been returned. Only valid after {@link #startDrain()}.
     */
    public Page pollNextDrainedPage()
    {
        checkState(draining, "Drain has not been started");
        while (true) {
            if (resolver != null) {
                Page page = resolver.next();
                if (page != null) {
                    return page;
                }
                // the partition is fully resolved, so its spill files are not needed any more
                resolver.close();
                resolver = null;
                closePartition(resolverPartition);
            }
            int partition = nextColdPartitionToResolve();
            if (partition < 0) {
                return null;
            }
            resolverPartition = partition;
            resolver = new PartitionResolver(partition);
        }
    }

    private int nextColdPartitionToResolve()
    {
        if (cold == null) {
            return -1;
        }
        while (drainCursor < cold.length) {
            int partition = drainCursor++;
            if (cold[partition] && (seeds[partition] != null || raws[partition] != null)) {
                return partition;
            }
        }
        return -1;
    }

    private void closePartition(int partition)
    {
        try (Closer closer = Closer.create()) {
            if (seeds[partition] != null) {
                closer.register(seeds[partition]);
                seeds[partition] = null;
            }
            if (raws[partition] != null) {
                closer.register(raws[partition]);
                raws[partition] = null;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final class PartitionResolver
            implements Closeable
    {
        private enum State
        {
            SEEDING, RAW_ROWS, DRAINING, DONE
        }

        private final SpillableMarkDistinctHash nested;
        private final Iterator<Iterator<Page>> seedSpills;
        private final Iterator<Iterator<Page>> rawSpills;
        private Iterator<Page> currentSeeds = Collections.emptyIterator();
        private Iterator<Page> currentRaw = Collections.emptyIterator();
        private State state = State.SEEDING;

        private PartitionResolver(int partition)
        {
            // The nested instance never yields on memory: its size is reported through the owning operator
            nested = new SpillableMarkDistinctHash(
                    session,
                    distinctTypes,
                    rawTypes,
                    markDistinctChannels,
                    hashStrategyCompiler,
                    () -> true,
                    true,
                    spillerFactory,
                    operatorContext,
                    DataSize.ofBytes(maxResolveBytes),
                    depth + 1);
            seedSpills = spillsOf(seeds[partition]);
            rawSpills = spillsOf(raws[partition]);
        }

        private Iterator<Iterator<Page>> spillsOf(PartitionSpill spill)
        {
            if (spill == null) {
                return Collections.emptyIterator();
            }
            return spill.spills().iterator();
        }

        private long estimatedSize()
        {
            return nested.getEstimatedSize();
        }

        private Page next()
        {
            while (true) {
                switch (state) {
                    case SEEDING -> {
                        if (currentSeeds.hasNext()) {
                            nested.seed(currentSeeds.next());
                            nested.splitIfNeeded();
                        }
                        else if (seedSpills.hasNext()) {
                            currentSeeds = seedSpills.next();
                        }
                        else {
                            state = State.RAW_ROWS;
                        }
                    }
                    case RAW_ROWS -> {
                        if (currentRaw.hasNext()) {
                            Page resolved = nested.resolve(currentRaw.next());
                            nested.splitIfNeeded();
                            if (resolved != null && resolved.getPositionCount() > 0) {
                                return resolved;
                            }
                        }
                        else if (rawSpills.hasNext()) {
                            currentRaw = rawSpills.next();
                        }
                        else {
                            nested.startDrain();
                            state = State.DRAINING;
                        }
                    }
                    case DRAINING -> {
                        Page page = nested.pollNextDrainedPage();
                        if (page != null) {
                            return page;
                        }
                        state = State.DONE;
                    }
                    case DONE -> {
                        return null;
                    }
                }
            }
        }

        @Override
        public void close()
        {
            try {
                nested.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Rows of one cold partition. They are buffered and written to the spiller of the partition in larger
     * chunks, so that one spill file holds many pages.
     */
    private final class PartitionSpill
            implements Closeable
    {
        private final List<Type> types;
        private final List<Page> buffered = new ArrayList<>();
        private long bufferedSize;
        private Spiller spiller;

        private PartitionSpill(List<Type> types)
        {
            this.types = types;
        }

        private void add(Page page)
        {
            long size = page.getRetainedSizeInBytes();
            buffered.add(page);
            bufferedSize += size;
            bufferedBytes += size;
            if (bufferedSize >= SPILL_BUFFER_BYTES) {
                flush();
            }
        }

        private void flush()
        {
            if (buffered.isEmpty()) {
                return;
            }
            if (spiller == null) {
                spiller = spillerFactory.create(types, operatorContext.getSpillContext().newLocalSpillContext(), operatorContext.newAggregateUserMemoryContext());
            }
            getFutureValue(spiller.spill(ImmutableList.copyOf(buffered).iterator()));
            clearBuffer();
        }

        private List<Iterator<Page>> spills()
        {
            flush();
            if (spiller == null) {
                return ImmutableList.of();
            }
            return spiller.getSpills();
        }

        private void clearBuffer()
        {
            bufferedBytes -= bufferedSize;
            bufferedSize = 0;
            buffered.clear();
        }

        @Override
        public void close()
        {
            clearBuffer();
            if (spiller != null) {
                spiller.close();
                spiller = null;
            }
        }
    }

    /**
     * Drives a {@link Work} instance to completion synchronously. Only used where memory updates never
     * report a shortage (the rebuilt hash and nested resolutions), so the work completes in one call.
     */
    private static <T> T forceProcess(Work<T> work)
    {
        while (!work.process()) {
            // see javadoc: not expected to be entered
        }
        return work.getResult();
    }

    private static HashGenerator saltedHashGenerator(HashGenerator delegate, long salt)
    {
        if (salt == 0) {
            return delegate;
        }
        return new HashGenerator()
        {
            @Override
            public long hashPosition(int position, Page page)
            {
                return mix(delegate.hashPosition(position, page), salt);
            }

            @Override
            public void hash(Page page, int positionOffset, int length, long[] hashes)
            {
                delegate.hash(page, positionOffset, length, hashes);
                for (int i = 0; i < length; i++) {
                    hashes[i] = mix(hashes[i], salt);
                }
            }

            private long mix(long hash, long s)
            {
                return Long.rotateLeft(hash, (int) (s * 13 % 61)) ^ (s * 0x9E3779B97F4A7C15L);
            }
        };
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            if (resolver != null) {
                closer.register(resolver);
            }
            if (cold != null) {
                for (int partition = 0; partition < cold.length; partition++) {
                    if (seeds[partition] != null) {
                        closer.register(seeds[partition]);
                    }
                    if (raws[partition] != null) {
                        closer.register(raws[partition]);
                    }
                }
            }
        }
        finally {
            resolver = null;
            hotHash = null;
        }
    }
}
