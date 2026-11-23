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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.type.BigintType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SystemSessionProperties.MARK_DISTINCT_SPILL_ENABLED;
import static io.trino.SystemSessionProperties.SPILL_ENABLED;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A spilled MarkDistinct emits the rows of spilled partitions after all other rows, so it must not
 * advertise the sorting or grouping of its source when spilling is enabled for the session.
 */
public class TestMarkDistinctSpillProperties
{
    private final PlannerContext plannerContext = plannerContextBuilder().build();
    private final SymbolAllocator symbolAllocator = new SymbolAllocator(ImmutableList.of());
    private final Symbol a = symbolAllocator.newSymbol("a", BigintType.BIGINT);
    private final Symbol b = symbolAllocator.newSymbol("b", BigintType.BIGINT);
    private final Symbol marker = symbolAllocator.newSymbol("marker", io.trino.spi.type.BooleanType.BOOLEAN);

    @Test
    public void testLocalPropertiesAreKeptWithoutSpill()
    {
        assertThat(deriveLocalProperties(session(false, false))).isNotEmpty();
        // spill_enabled alone does not change the operator
        assertThat(deriveLocalProperties(session(true, false))).isNotEmpty();
        assertThat(deriveLocalProperties(session(false, true))).isNotEmpty();
    }

    @Test
    public void testLocalPropertiesAreDroppedWithSpill()
    {
        assertThat(deriveLocalProperties(session(true, true))).isEmpty();
    }

    @Test
    public void testStreamOrderingIsDroppedWithSpill()
    {
        assertThat(deriveStreamOrdering(session(false, false))).isTrue();
        assertThat(deriveStreamOrdering(session(true, false))).isTrue();
        assertThat(deriveStreamOrdering(session(true, true))).isFalse();
    }

    private List<?> deriveLocalProperties(Session session)
    {
        ValuesNode values = new ValuesNode(new PlanNodeId("values"), ImmutableList.of(a, b));
        MarkDistinctNode node = new MarkDistinctNode(new PlanNodeId("markDistinct"), values, marker, ImmutableList.of(a));
        ActualProperties source = ActualProperties.builder()
                .local(ImmutableList.of(new GroupingProperty<>(ImmutableList.of(a)), new SortingProperty<>(b, SortOrder.ASC_NULLS_FIRST)))
                .build();
        return PropertyDerivations.deriveProperties(node, ImmutableList.of(source), plannerContext, session, symbolAllocator).getLocalProperties();
    }

    private boolean deriveStreamOrdering(Session session)
    {
        ValuesNode values = new ValuesNode(new PlanNodeId("values"), ImmutableList.of(a, b));
        StreamProperties valuesProperties = StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(values, ImmutableList.of(), plannerContext.getMetadata(), session);
        SortNode sort = new SortNode(new PlanNodeId("sort"), values, new OrderingScheme(ImmutableList.of(a), java.util.Map.of(a, SortOrder.ASC_NULLS_FIRST)), false);
        StreamProperties sorted = StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(sort, ImmutableList.of(valuesProperties), plannerContext.getMetadata(), session);
        assertThat(sorted.isOrdered()).isTrue();
        MarkDistinctNode node = new MarkDistinctNode(new PlanNodeId("markDistinct"), sort, marker, ImmutableList.of(a));
        return StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties(node, ImmutableList.of(sorted), plannerContext.getMetadata(), session).isOrdered();
    }

    private static Session session(boolean spillEnabled, boolean markDistinctSpillEnabled)
    {
        return testSessionBuilder()
                .setSystemProperty(SPILL_ENABLED, String.valueOf(spillEnabled))
                .setSystemProperty(MARK_DISTINCT_SPILL_ENABLED, String.valueOf(markDistinctSpillEnabled))
                .build();
    }
}
