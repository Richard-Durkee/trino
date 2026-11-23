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
import io.trino.FeaturesConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.type.Type;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.NodeSpillConfig;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;

/**
 * A spiller factory that really serializes pages to files in a temporary directory, unlike
 * {@link DummySpillerFactory} which keeps the page objects. It also counts spill calls and live files.
 */
public class TestingFileSpillerFactory
        implements SpillerFactory, AutoCloseable
{
    private final Path spillPath;
    private final FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private final SpillerFactory delegate;
    private final AtomicInteger spillCalls = new AtomicInteger();

    public TestingFileSpillerFactory()
    {
        try {
            spillPath = Files.createTempDirectory("mark-distinct-spill");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(ImmutableList.of(spillPath.toAbsolutePath().toString()));
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(TESTING_BLOCK_ENCODING_SERDE, new SpillerStats(), featuresConfig, new NodeSpillConfig(), new TaskManagerConfig());
        delegate = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @Override
    public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
    {
        Spiller spiller = delegate.create(types, spillContext, memoryContext);
        return new Spiller()
        {
            @Override
            public com.google.common.util.concurrent.ListenableFuture<io.airlift.units.DataSize> spill(java.util.Iterator<io.trino.spi.Page> pageIterator)
            {
                spillCalls.incrementAndGet();
                return spiller.spill(pageIterator);
            }

            @Override
            public List<java.util.Iterator<io.trino.spi.Page>> getSpills()
            {
                return spiller.getSpills();
            }

            @Override
            public void close()
            {
                spiller.close();
            }
        };
    }

    public int spillCalls()
    {
        return spillCalls.get();
    }

    public long liveSpillFiles()
    {
        try (Stream<Path> files = Files.walk(spillPath)) {
            return files.filter(Files::isRegularFile).count();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        singleStreamSpillerFactory.destroy();
        deleteRecursively(spillPath, ALLOW_INSECURE);
    }
}
