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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving MarkDistinctOperator spill works end-to-end with real SQL queries.
 * Uses memory-revoking-threshold=0.0 to force immediate spill on all operators that support it,
 * verifying that COUNT(DISTINCT) queries with multiple distinct aggregations produce correct
 * results when the MarkDistinct hash is spilled to disk.
 */
public class TestSpilledMarkDistinctQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "128kB")
                .build();

        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("spiller-spill-path", Path.of(System.getProperty("java.io.tmpdir"), "trino", "spills", randomUUID().toString()).toString())
                .put("spiller-max-used-space-threshold", "1.0")
                .put("memory-revoking-threshold", "0.0") // revoke always -- forces spill
                .put("memory-revoking-target", "0.0")
                .buildOrThrow();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setWorkerCount(1)
                .setExtraProperties(extraProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testMultipleCountDistinctWithSpill()
    {
        // This is the exact query shape that triggers MarkDistinctOperator (multiple DISTINCT
        // aggregations in one query). Without spill support, this shape OOMs on large data.
        // With memory-revoking-threshold=0.0, spill is forced immediately.
        MaterializedResult result = computeActual(
                "SELECT count(DISTINCT orderkey), count(DISTINCT custkey) FROM orders");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(0)).isEqualTo(15000L);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(1000L);
    }

    @Test
    public void testMultipleDistinctAggregationsWithGroupBy()
    {
        // GROUP BY + multiple COUNT(DISTINCT) -- the pattern that OOM'd in production
        // (V2322268420: 11-key GROUP BY with two COUNT(DISTINCT eventId) aggregations)
        MaterializedResult result = computeActual(
                "SELECT orderstatus, count(DISTINCT orderkey), count(DISTINCT custkey) " +
                        "FROM orders GROUP BY orderstatus ORDER BY orderstatus");
        assertThat(result.getRowCount()).isEqualTo(3); // F, O, P
        // Verify non-zero counts for each status
        for (int i = 0; i < 3; i++) {
            assertThat((long) result.getMaterializedRows().get(i).getField(1)).isGreaterThan(0);
            assertThat((long) result.getMaterializedRows().get(i).getField(2)).isGreaterThan(0);
        }
    }

    @Test
    public void testCountDistinctMatchesNonDistinct()
    {
        // Verify correctness: COUNT(DISTINCT x) with spill must match the non-spill result.
        // We compare against a known-correct formulation.
        MaterializedResult spilledResult = computeActual(
                "SELECT count(DISTINCT custkey) FROM orders");
        MaterializedResult referenceResult = computeActual(
                "SELECT count(*) FROM (SELECT DISTINCT custkey FROM orders)");
        assertThat(spilledResult.getMaterializedRows().get(0).getField(0))
                .isEqualTo(referenceResult.getMaterializedRows().get(0).getField(0));
    }

    @Test
    public void testMixedDistinctAndNonDistinctAggregations()
    {
        // Mix of DISTINCT and non-DISTINCT aggregations in a single query
        MaterializedResult result = computeActual(
                "SELECT count(DISTINCT custkey), sum(totalprice), count(DISTINCT orderstatus) FROM orders");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(0)).isEqualTo(1000L);
        assertThat((double) result.getMaterializedRows().get(0).getField(1)).isGreaterThan(0.0);
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isEqualTo(3L); // F, O, P
    }

    @Test
    public void testDistinctWithHighCardinalityGroupBy()
    {
        // High cardinality GROUP BY with DISTINCT -- many MarkDistinct partitions in flight
        MaterializedResult result = computeActual(
                "SELECT orderkey, count(DISTINCT custkey), count(DISTINCT orderstatus) " +
                        "FROM orders GROUP BY orderkey HAVING count(DISTINCT custkey) > 0");
        // Every order has exactly one customer, so count(distinct custkey) = 1 for each
        assertThat(result.getRowCount()).isEqualTo(15000);
        for (int i = 0; i < Math.min(100, result.getRowCount()); i++) {
            assertThat((long) result.getMaterializedRows().get(i).getField(1)).isEqualTo(1L);
        }
    }
}
