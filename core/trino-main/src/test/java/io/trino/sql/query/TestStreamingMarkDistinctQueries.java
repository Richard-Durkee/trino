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
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.SPILL_ENABLED;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Runs the check for multiple rows of a correlated scalar subquery with several drivers per task and with spilling,
 * and verifies which mark distinct operator was used.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStreamingMarkDistinctQueries
{
    private static final String SUBQUERY = "SELECT l.orderkey, l.linenumber, (SELECT n.name FROM nation n WHERE n.nationkey = l.suppkey % 25) FROM lineitem l";
    private static final String EQUIVALENT_JOIN = "SELECT l.orderkey, l.linenumber, n.name FROM lineitem l LEFT JOIN nation n ON n.nationkey = l.suppkey % 25";

    private final StandaloneQueryRunner runner;
    private final QueryAssertions assertions;

    public TestStreamingMarkDistinctQueries()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "4"));
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSeveralDriversPerTask()
    {
        for (String taskConcurrency : new String[] {"1", "4", "16"}) {
            Session session = Session.builder(runner.getDefaultSession())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                    .setSystemProperty(TASK_CONCURRENCY, taskConcurrency)
                    .build();
            assertThat(operatorTypes(session, SUBQUERY)).contains("StreamingMarkDistinctOperator").doesNotContain("MarkDistinctOperator");
            assertThat(assertions.query(session, SUBQUERY)).matches(EQUIVALENT_JOIN);
        }
    }

    @Test
    public void testPartitionedJoinUsesMarkDistinct()
    {
        Session session = Session.builder(runner.getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
        assertThat(operatorTypes(session, SUBQUERY)).contains("MarkDistinctOperator").doesNotContain("StreamingMarkDistinctOperator");
        assertThat(assertions.query(session, SUBQUERY)).matches(EQUIVALENT_JOIN);
    }

    @Test
    public void testSpillEnabled()
    {
        Session session = Session.builder(runner.getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .setSystemProperty(SPILL_ENABLED, "true")
                .build();
        // a join that can spill drops the grouping of its probe side, and keeps columns that were constant within that grouping
        assertThat(operatorTypes(session, SUBQUERY)).contains("MarkDistinctOperator").doesNotContain("StreamingMarkDistinctOperator");
        assertThat(assertions.query(session, SUBQUERY)).matches(EQUIVALENT_JOIN);
    }

    private Set<String> operatorTypes(Session session, String sql)
    {
        MaterializedResultWithPlan result = runner.executeWithPlan(session, sql);
        MaterializedResult rows = result.result();
        assertThat(rows.getRowCount()).isGreaterThan(0);
        QueryId queryId = result.queryId();
        QueryInfo info = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);
        return info.getQueryStats().getOperatorSummaries().stream()
                .map(stats -> stats.getOperatorType())
                .collect(Collectors.toSet());
    }
}
