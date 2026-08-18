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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;

import static io.trino.SystemSessionProperties.SPILL_ENABLED;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Integration test proving that multiple COUNT(DISTINCT) queries produce correct results
 * when MarkDistinctOperator is forced to spill. Uses memory-revoking-threshold=0.0 to
 * trigger spill on every memory-revocable operator, including our new MarkDistinct spill path.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMarkDistinctSpill
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(SPILL_ENABLED, "true")
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .build();

        StandaloneQueryRunner runner = new StandaloneQueryRunner(session, builder -> builder.setProperties(
                ImmutableMap.<String, String>builder()
                        .put("query.client.timeout", "10m")
                        .put("exchange.http-client.idle-timeout", "1h")
                        .put("node-scheduler.min-candidates", "1")
                        .put("task.info.max-age", "10s")
                        .put("task.info-update-interval", "1s")
                        .put("spiller-spill-path", Path.of(System.getProperty("java.io.tmpdir"), "trino", "spills", randomUUID().toString()).toString())
                        .put("spiller-max-used-space-threshold", "1.0")
                        .put("memory-revoking-threshold", "0.0") // force spill on every revocable operator
                        .put("memory-revoking-target", "0.0")
                        .buildOrThrow()));
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of());
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testMultipleCountDistinct()
    {
        // Multiple DISTINCT aggregations force MarkDistinctNode in the plan.
        // With memory-revoking-threshold=0.0, the MarkDistinctOperator is forced to spill.
        assertThat(assertions.query(
                "SELECT count(DISTINCT nationkey), count(DISTINCT regionkey) FROM tpch.tiny.nation"))
                .matches("VALUES (BIGINT '25', BIGINT '5')");
    }

    @Test
    public void testMultipleCountDistinctWithGroupBy()
    {
        // GROUP BY + multiple COUNT(DISTINCT) -- the real-world OOM pattern
        assertThat(assertions.query(
                "SELECT regionkey, count(DISTINCT nationkey), count(DISTINCT name) " +
                        "FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey"))
                .matches("VALUES " +
                        "(BIGINT '0', BIGINT '5', BIGINT '5'), " +
                        "(BIGINT '1', BIGINT '5', BIGINT '5'), " +
                        "(BIGINT '2', BIGINT '5', BIGINT '5'), " +
                        "(BIGINT '3', BIGINT '5', BIGINT '5'), " +
                        "(BIGINT '4', BIGINT '5', BIGINT '5')");
    }

    @Test
    public void testCountDistinctCorrectness()
    {
        // Verify spilled COUNT(DISTINCT) matches an equivalent subquery formulation
        assertThat(assertions.query(
                "SELECT count(DISTINCT regionkey) FROM tpch.tiny.nation"))
                .matches("SELECT count(*) FROM (SELECT DISTINCT regionkey FROM tpch.tiny.nation)");
    }

    @Test
    public void testMixedDistinctAndNonDistinct()
    {
        // Mixed DISTINCT + non-DISTINCT aggregations in one query
        assertThat(assertions.query(
                "SELECT count(DISTINCT regionkey), count(*), count(DISTINCT nationkey) FROM tpch.tiny.nation"))
                .matches("VALUES (BIGINT '5', BIGINT '25', BIGINT '25')");
    }
}
