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
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Mark distinct above GroupId, whose output repeats every input page once per grouping set, so rows with equal values
 * are not adjacent even if the input of GroupId is grouped on them.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMarkDistinctOverGroupId
{
    private static final String GROUPED_INPUT = "(SELECT custkey, orderstatus, orderpriority, clerk, row_number() OVER (PARTITION BY custkey ORDER BY orderkey) rn FROM orders)";

    private final QueryAssertions assertions;

    public TestMarkDistinctOverGroupId()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .build();
        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testDistinctAggregationsOverGroupingSetsOfGroupedInput()
    {
        // the window groups its output on custkey and the grouping sets all contain custkey
        assertThat(assertions.query(
                "SELECT custkey, orderstatus, count(DISTINCT orderpriority), count(DISTINCT clerk), max(rn) " +
                        "FROM " + GROUPED_INPUT + " GROUP BY GROUPING SETS ((custkey), (custkey, orderstatus))"))
                .matches(
                        "SELECT custkey, CAST(NULL AS varchar(1)), count(DISTINCT orderpriority), count(DISTINCT clerk), max(rn) FROM " + GROUPED_INPUT + " GROUP BY custkey " +
                                "UNION ALL " +
                                "SELECT custkey, orderstatus, count(DISTINCT orderpriority), count(DISTINCT clerk), max(rn) FROM " + GROUPED_INPUT + " GROUP BY custkey, orderstatus");
    }
}
