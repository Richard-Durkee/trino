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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

/**
 * Adds hash-partitioned ("grace hash" / hybrid hash) external spilling on top of {@link MarkDistinctHash}.
 * <p>
 * {@link MarkDistinctOperator} must remember every distinct key it has ever seen for the lifetime of the
 * operator, which today is tracked purely in-memory via {@link MarkDistinctHash}: there is no relief valve
 * when the distinct key set exceeds the per-node memory limit.
 * <p>
 * This class hash-partitions the distinct key space into a fixed number of buckets. Because all
 * occurrences of a given key always land in the same bucket regardless of arrival time, a bucket that is
 * spilled to disk ("cold") can safely defer every one of its rows' distinct decisions to a later, one-shot
 * resolution pass at {@code finish()} time, while buckets that remain resident ("hot") keep resolving rows
 * immediately. Row order across the operator's output is not a correctness contract for MarkDistinct.
 */
public class SpillableMarkDistinctHash
        implements AutoCloseable
{
    @VisibleForTesting
    static final int DEFAULT_PARTITION_COUNT = 32;
    @VisibleForTesting
    static final int RECURSIVE_PARTITION_COUNT = 8;
    private static final int MAX_RECURSION_DEPTH = 4;

    private final Session session;
    private final List<Type> distinctTypes;
    private final List<Type> rawTypes;
    private final int[] markDistinctChannels;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final UpdateMemory updateMemory;
    private final boolean spillEnabled;
    private final SpillerFactory spillerFactory;
    private final OperatorContext operatorContext;
    private final long maxPartitionResolveSizeBytes;
    private final long recursionSalt;

    private MarkDistinctHash hotHash;

    private HashGenerator hashGenerator;
    private LocalPartitionGenerator partitionFunction;
    private int partitionCount;
    private boolean[] cold;
    private Spiller[] seedSpillers;
    private Spiller[] rawSpillers;
    private long[] partitionBytes;

    private int drainCursor;
    // Drain state: we resolve one partition at a time, and within a partition we emit one page
    // per pollNextDrainedPage() call to avoid buffering the entire partition in memory.
    private MarkDistinctHash drainResolveHash;
    private Iterator<Page> drainRawBatchIterator;
    private List<Iterator<Page>> drainRawSpillIterators;
    private int drainRawIteratorIndex;
    private final Deque<Page> drainQueue = new ArrayDeque<>();

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
            DataSize maxPartitionResolveSize)
    {
        this(session, distinctTypes, rawTypes, markDistinctChannels, hashStrategyCompiler,
                updateMemory, spillEnabled, spillerFactory, operatorContext, maxPartitionResolveSize, 0);
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
            DataSize maxPartitionResolveSize,
            long recursionSalt)
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
        this.maxPartitionResolveSizeBytes = maxPartitionResolveSize.toBytes();
        this.recursionSalt = recursionSalt;
        this.hotHash = new MarkDistinctHash(session, distinctTypes, hashStrategyCompiler, updateMemory);
    }

    public long getEstimatedSize()
    {
        return hotHash.getEstimatedSize();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return hotHash.getCapacity();
    }

    @VisibleForTesting
    boolean isSpilled()
    {
        return hashGenerator != null;
    }

    public Work<Page> markDistinctRows(Page fullPage)
    {
        Page distinctColumnsPage = fullPage.getColumns(markDistinctChannels);

        if (hashGenerator == null) {
            Work<Block> work = hotHash.markDistinctRows(distinctColumnsPage);
            return new TransformWork<>(work, block -> fullPage.appendColumn(block));
        }

        int positionCount = fullPage.getPositionCount();
        int[] partitions = new int[positionCount];
        partitionFunction.getPartitions(distinctColumnsPage, 0, positionCount, partitions);

        IntArrayList hotPositions = new IntArrayList();
        Map<Integer, IntArrayList> coldPositionsByPartition = new HashMap<>();
        for (int position = 0; position < positionCount; position++) {
            int partition = partitions[position];
            if (cold[partition]) {
                coldPositionsByPartition.computeIfAbsent(partition, ignored -> new IntArrayList()).add(position);
            }
            else {
                hotPositions.add(position);
            }
        }

        for (Map.Entry<Integer, IntArrayList> entry : coldPositionsByPartition.entrySet()) {
            IntArrayList positions = entry.getValue();
            spillRaw(entry.getKey(), fullPage.getPositions(positions.elements(), 0, positions.size()));
        }

        if (hotPositions.isEmpty()) {
            return NO_OUTPUT_WORK;
        }

        int[] hotPositionsArray = hotPositions.elements();
        int hotPositionsCount = hotPositions.size();
        Page hotFullPage = fullPage.getPositions(hotPositionsArray, 0, hotPositionsCount);
        Page hotDistinctPage = distinctColumnsPage.getPositions(hotPositionsArray, 0, hotPositionsCount);
        Work<Block> work = hotHash.markDistinctRows(hotDistinctPage);
        return new TransformWork<>(work, block -> hotFullPage.appendColumn(block));
    }

    private static final Work<Page> NO_OUTPUT_WORK = new Work<>()
    {
        @Override
        public boolean process()
        {
            return true;
        }

        @Override
        public Page getResult()
        {
            return null;
        }
    };

    public ListenableFuture<Void> startMemoryRevoke()
    {
        if (hashGenerator == null) {
            initializePartitioning(DEFAULT_PARTITION_COUNT);
        }
        else {
            demotePartitions(Math.max(1, countHotPartitions() / 2));
        }
        return immediateVoidFuture();
    }

    public void finishMemoryRevoke()
    {
    }

    private void initializePartitioning(int newPartitionCount)
    {
        this.partitionCount = newPartitionCount;
        this.hashGenerator = saltedHashGenerator(hashStrategyCompiler.getInterpretedHashGenerator(distinctTypes), recursionSalt);
        this.partitionFunction = new LocalPartitionGenerator(hashGenerator, partitionCount);
        this.cold = new boolean[partitionCount];
        this.seedSpillers = new Spiller[partitionCount];
        this.rawSpillers = new Spiller[partitionCount];
        this.partitionBytes = new long[partitionCount];
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
     * Dumps the entire resident key set, re-partitions it, spills the rows belonging to the {@code count}
     * partitions chosen to be newly demoted (plus any rows for partitions that were already cold), and
     * rebuilds a fresh, smaller resident hash containing only the rows for partitions that remain hot.
     * <p>
     * <strong>Cost:</strong> O(n) in the number of resident keys — allocates a temporary Page holding all
     * keys, partitions it, then rebuilds the hash. The first revoke is the most expensive (full hash dump);
     * subsequent revokes are cheaper because the resident hash shrinks with each demotion. For a 10M-key
     * hash this may take tens of milliseconds, but it runs at most log2(32) = 5 times before all partitions
     * are cold, and it only runs during the memory-revoke critical path where the alternative is OOM.
     */
    private void demotePartitions(int count)
    {
        List<Integer> hotPartitionIndexes = new java.util.ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            if (!cold[partition]) {
                hotPartitionIndexes.add(partition);
            }
        }
        int toDemote = Math.min(count, Math.max(0, hotPartitionIndexes.size() - 1));
        for (int i = hotPartitionIndexes.size() - toDemote; i < hotPartitionIndexes.size(); i++) {
            cold[hotPartitionIndexes.get(i)] = true;
        }

        Page residentKeys = hotHash.dumpAllGroups(distinctTypes);
        int positionCount = residentKeys.getPositionCount();
        int[] partitions = new int[positionCount];
        partitionFunction.getPartitions(residentKeys, 0, positionCount, partitions);

        IntArrayList hotPositions = new IntArrayList();
        Map<Integer, IntArrayList> coldPositionsByPartition = new HashMap<>();
        for (int position = 0; position < positionCount; position++) {
            int partition = partitions[position];
            if (cold[partition]) {
                coldPositionsByPartition.computeIfAbsent(partition, ignored -> new IntArrayList()).add(position);
            }
            else {
                hotPositions.add(position);
            }
        }

        for (Map.Entry<Integer, IntArrayList> entry : coldPositionsByPartition.entrySet()) {
            IntArrayList positions = entry.getValue();
            spillSeed(entry.getKey(), residentKeys.getPositions(positions.elements(), 0, positions.size()));
        }

        MarkDistinctHash rebuilt = new MarkDistinctHash(session, distinctTypes, hashStrategyCompiler, updateMemory);
        if (!hotPositions.isEmpty()) {
            Page hotSeedPage = residentKeys.getPositions(hotPositions.elements(), 0, hotPositions.size());
            forceProcess(rebuilt.markDistinctRows(hotSeedPage));
        }
        hotHash = rebuilt;
    }

    private void spillSeed(int partition, Page page)
    {
        if (page.getPositionCount() == 0) {
            return;
        }
        if (seedSpillers[partition] == null) {
            seedSpillers[partition] = spillerFactory.create(distinctTypes, operatorContext.getSpillContext().newLocalSpillContext(), operatorContext.newAggregateUserMemoryContext());
        }
        partitionBytes[partition] += page.getSizeInBytes();
        getFutureValue(seedSpillers[partition].spill(List.of(page).iterator()));
    }

    private void spillRaw(int partition, Page page)
    {
        // TODO: We spill the full row (all source columns) because the drain phase must emit complete
        // output pages. An alternative would be spilling only the distinct columns + a row position
        // index, then reconstructing full rows from the upstream source at drain time — but the
        // upstream source is consumed and gone by finish(). Spilling only distinct columns is feasible
        // if MarkDistinct's downstream consumer only needs those columns (which is sometimes true for
        // COUNT(DISTINCT)), but that would require plan-level awareness of which columns are actually
        // needed downstream. Left as a future optimization.
        if (page.getPositionCount() == 0) {
            return;
        }
        if (rawSpillers[partition] == null) {
            rawSpillers[partition] = spillerFactory.create(rawTypes, operatorContext.getSpillContext().newLocalSpillContext(), operatorContext.newAggregateUserMemoryContext());
        }
        partitionBytes[partition] += page.getSizeInBytes();
        getFutureValue(rawSpillers[partition].spill(List.of(page).iterator()));
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

    public Page pollNextDrainedPage()
    {
        // Emit buffered pages first (from recursive resolution or prior iteration)
        if (!drainQueue.isEmpty()) {
            return drainQueue.poll();
        }

        // Continue resolving the current partition lazily (one page at a time)
        if (drainResolveHash != null && drainRawBatchIterator != null) {
            Page page = drainNextFromCurrentPartition();
            if (page != null) {
                return page;
            }
        }

        // Find and start resolving the next cold partition
        while (true) {
            int partition = nextColdPartitionToResolve();
            if (partition < 0) {
                return null;
            }

            if (partitionBytes[partition] > maxPartitionResolveSizeBytes) {
                // Partition too large for single-pass resolution; recursively sub-partition it.
                // Recursive resolution buffers into drainQueue (bounded by sub-partition size).
                recursivelyResolve(partition, 0);
                if (!drainQueue.isEmpty()) {
                    return drainQueue.poll();
                }
                continue;
            }

            // Seed the resolve hash with pre-spill keys
            drainResolveHash = new MarkDistinctHash(session, distinctTypes, hashStrategyCompiler, () -> true);
            if (seedSpillers[partition] != null) {
                for (Iterator<Page> spillBatch : seedSpillers[partition].getSpills()) {
                    while (spillBatch.hasNext()) {
                        forceProcess(drainResolveHash.markDistinctRows(spillBatch.next()));
                    }
                }
            }

            // Set up lazy raw-page iteration for this partition
            if (rawSpillers[partition] != null) {
                drainRawSpillIterators = rawSpillers[partition].getSpills();
                drainRawIteratorIndex = 0;
                drainRawBatchIterator = drainRawSpillIterators.isEmpty() ? null : drainRawSpillIterators.get(0);
            }
            else {
                drainRawBatchIterator = null;
            }

            Page page = drainNextFromCurrentPartition();
            if (page != null) {
                return page;
            }
            // This partition had seeds but no raw rows — advance to next
        }
    }

    /**
     * Resolves and returns exactly one page from the current partition's raw spill iterator,
     * or null if the current partition is fully drained.
     */
    private Page drainNextFromCurrentPartition()
    {
        while (drainRawBatchIterator != null) {
            if (drainRawBatchIterator.hasNext()) {
                Page rawPage = drainRawBatchIterator.next();
                Page distinctColumnsPage = rawPage.getColumns(markDistinctChannels);
                Block marker = forceProcess(drainResolveHash.markDistinctRows(distinctColumnsPage));
                return rawPage.appendColumn(marker);
            }
            // Advance to next spill batch
            drainRawIteratorIndex++;
            if (drainRawSpillIterators != null && drainRawIteratorIndex < drainRawSpillIterators.size()) {
                drainRawBatchIterator = drainRawSpillIterators.get(drainRawIteratorIndex);
            }
            else {
                drainRawBatchIterator = null;
                drainRawSpillIterators = null;
                drainResolveHash = null;
            }
        }
        return null;
    }

    private int nextColdPartitionToResolve()
    {
        while (drainCursor < partitionCount) {
            int partition = drainCursor;
            drainCursor++;
            if (cold[partition] && (seedSpillers[partition] != null || rawSpillers[partition] != null)) {
                return partition;
            }
        }
        return -1;
    }

    /**
     * Eagerly resolves a partition into the provided queue. Used only by recursive resolution
     * (where the partition is bounded by maxPartitionResolveSizeBytes / RECURSIVE_PARTITION_COUNT
     * and thus safe to buffer in memory).
     */
    private void resolvePartitionIntoQueue(int partition, int depth)
    {
        if (depth < MAX_RECURSION_DEPTH && partitionBytes[partition] > maxPartitionResolveSizeBytes) {
            recursivelyResolve(partition, depth);
            return;
        }

        MarkDistinctHash resolveHash = new MarkDistinctHash(session, distinctTypes, hashStrategyCompiler, () -> true);
        if (seedSpillers[partition] != null) {
            for (Iterator<Page> spillBatch : seedSpillers[partition].getSpills()) {
                while (spillBatch.hasNext()) {
                    forceProcess(resolveHash.markDistinctRows(spillBatch.next()));
                }
            }
        }
        if (rawSpillers[partition] != null) {
            for (Iterator<Page> spillBatch : rawSpillers[partition].getSpills()) {
                while (spillBatch.hasNext()) {
                    Page rawPage = spillBatch.next();
                    Page distinctColumnsPage = rawPage.getColumns(markDistinctChannels);
                    Block marker = forceProcess(resolveHash.markDistinctRows(distinctColumnsPage));
                    drainQueue.add(rawPage.appendColumn(marker));
                }
            }
        }
    }

    private void recursivelyResolve(int partition, int depth)
    {
        try (SpillableMarkDistinctHash nested = new SpillableMarkDistinctHash(
                session, distinctTypes, rawTypes, markDistinctChannels, hashStrategyCompiler,
                () -> true, true, spillerFactory, operatorContext,
                DataSize.succinctBytes(maxPartitionResolveSizeBytes), recursionSalt + depth + 1)) {
            nested.initializePartitioning(RECURSIVE_PARTITION_COUNT);

            if (seedSpillers[partition] != null) {
                for (Iterator<Page> spillBatch : seedSpillers[partition].getSpills()) {
                    while (spillBatch.hasNext()) {
                        nested.seedOnly(spillBatch.next());
                    }
                }
            }
            if (rawSpillers[partition] != null) {
                for (Iterator<Page> spillBatch : rawSpillers[partition].getSpills()) {
                    while (spillBatch.hasNext()) {
                        Page rawPage = spillBatch.next();
                        Page resolved = forceProcess(nested.markDistinctRows(rawPage));
                        if (resolved != null) {
                            drainQueue.add(resolved);
                        }
                    }
                }
            }
            nested.drainAllInto(drainQueue, depth + 1);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void seedOnly(Page distinctColumnsPage)
    {
        if (hashGenerator == null) {
            forceProcess(hotHash.markDistinctRows(distinctColumnsPage));
            return;
        }
        int positionCount = distinctColumnsPage.getPositionCount();
        int[] partitions = new int[positionCount];
        partitionFunction.getPartitions(distinctColumnsPage, 0, positionCount, partitions);

        IntArrayList hotPositions = new IntArrayList();
        Map<Integer, IntArrayList> coldPositionsByPartition = new HashMap<>();
        for (int position = 0; position < positionCount; position++) {
            int partition = partitions[position];
            if (cold[partition]) {
                coldPositionsByPartition.computeIfAbsent(partition, ignored -> new IntArrayList()).add(position);
            }
            else {
                hotPositions.add(position);
            }
        }
        for (Map.Entry<Integer, IntArrayList> entry : coldPositionsByPartition.entrySet()) {
            IntArrayList positions = entry.getValue();
            spillSeed(entry.getKey(), distinctColumnsPage.getPositions(positions.elements(), 0, positions.size()));
        }
        if (!hotPositions.isEmpty()) {
            forceProcess(hotHash.markDistinctRows(distinctColumnsPage.getPositions(hotPositions.elements(), 0, hotPositions.size())));
        }
    }

    private void drainAllInto(Deque<Page> sink, int depth)
    {
        int partition;
        while ((partition = nextColdPartitionToResolve()) >= 0) {
            resolvePartitionIntoQueue(partition, depth);
            sink.addAll(drainQueue);
            drainQueue.clear();
        }
    }

    /**
     * Drives a {@link Work} instance to completion synchronously. This is safe here because
     * {@link MarkDistinctHash#markDistinctRows} always completes in a single {@code process()} call
     * when the underlying {@link GroupByHash} has sufficient memory — and in this class, we only
     * call {@code forceProcess} on pages that are already buffered (either from the resident hash
     * dump or from spill files), so memory for the hash insertion is already accounted for.
     * The loop is a defensive measure honoring the Work contract in case a future GroupByHash
     * implementation introduces yielding on rehash.
     */
    private static <T> T forceProcess(Work<T> work)
    {
        while (!work.process()) {
            // See javadoc above: in practice this loop body is never entered.
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
            if (seedSpillers != null) {
                for (Spiller spiller : seedSpillers) {
                    if (spiller != null) {
                        closer.register(spiller::close);
                    }
                }
            }
            if (rawSpillers != null) {
                for (Spiller spiller : rawSpillers) {
                    if (spiller != null) {
                        closer.register(spiller::close);
                    }
                }
            }
        }
    }
}
