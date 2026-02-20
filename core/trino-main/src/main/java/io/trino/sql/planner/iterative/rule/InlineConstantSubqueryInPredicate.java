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
package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.matching.Pattern.empty;
import static io.trino.sql.planner.plan.Patterns.Apply.correlation;
import static io.trino.sql.planner.plan.Patterns.applyNode;

/**
 * Transforms IN predicates with constant-valued subqueries into IN predicates with literal lists.
 * This enables predicate pushdown for queries like:
 * <pre>
 * WITH constants AS (SELECT * FROM UNNEST(ARRAY[1, 2, 3]))
 * SELECT * FROM orders WHERE orderkey IN (SELECT * FROM constants)
 * </pre>
 *
 * Fixes issue #10938: https://github.com/trinodb/trino/issues/10938
 */
public class InlineConstantSubqueryInPredicate
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(empty(correlation()));

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode node, Captures captures, Context context)
    {
        // Only handle IN predicates
        if (node.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        ApplyNode.SetExpression setExpression = node.getSubqueryAssignments().values().iterator().next();
        if (!(setExpression instanceof ApplyNode.In inPredicate)) {
            return Result.empty();
        }

        // Resolve the subquery through the Lookup to get the actual plan
        PlanNode subquery = context.getLookup().resolve(node.getSubquery());

        // Try to extract constant values from the subquery
        Optional<List<Expression>> constantValues = extractConstantValues(subquery, context);
        if (constantValues.isEmpty() || constantValues.get().isEmpty()) {
            return Result.empty();
        }

        // Get the type of the IN value to ensure type compatibility
        Type inValueType = inPredicate.value().type();

        // Cast constants to match the IN value type if needed
        List<Expression> typedConstants = new ArrayList<>();
        for (Expression constant : constantValues.get()) {
            if (constant instanceof Constant c) {
                if (c.value() == null) {
                    // For NULL constants, create a new NULL constant with the target type
                    typedConstants.add(new Constant(inValueType, null));
                }
                else if (!c.type().equals(inValueType)) {
                    // For non-NULL constants with different types, add a cast
                    typedConstants.add(new Cast(constant, inValueType));
                }
                else {
                    typedConstants.add(constant);
                }
            }
            else {
                typedConstants.add(constant);
            }
        }

        Expression newInExpression = new In(inPredicate.value().toSymbolReference(), typedConstants);

        // Create a ProjectNode that replaces the subquery with the inlined IN expression
        io.trino.sql.planner.plan.Assignments.Builder assignments = io.trino.sql.planner.plan.Assignments.builder();
        for (io.trino.sql.planner.Symbol symbol : node.getInput().getOutputSymbols()) {
            assignments.put(symbol, symbol.toSymbolReference());
        }
        assignments.put(
                node.getSubqueryAssignments().keySet().iterator().next(),
                newInExpression);

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        node.getInput(),
                        assignments.build()));
    }

    /**
     * Extracts constant values from simple subquery patterns:
     * - ValuesNode with constant rows
     * - UnnestNode with constant array
     * - ProjectNode over constant source
     */
    private Optional<List<Expression>> extractConstantValues(PlanNode subquery, Context context)
    {
        // Resolve GroupReferences
        PlanNode resolved = context.getLookup().resolve(subquery);

        // Try to extract directly
        Optional<List<Expression>> result = tryExtractDirect(resolved, context);
        if (result.isPresent()) {
            return result;
        }

        // If subquery is a ProjectNode, try looking at its source
        if (resolved instanceof ProjectNode projectNode) {
            return extractConstantValues(projectNode.getSource(), context);
        }

        return Optional.empty();
    }

    private Optional<List<Expression>> tryExtractDirect(PlanNode node, Context context)
    {
        // Pattern 1: ValuesNode with constants
        if (node instanceof ValuesNode valuesNode) {
            return extractFromValues(valuesNode);
        }

        // Pattern 2: UnnestNode -> (ProjectNode)* -> ValuesNode
        if (node instanceof UnnestNode unnestNode) {
            PlanNode unnestSource = context.getLookup().resolve(unnestNode.getSource());

            // For UNNEST, we need to find where the array constant is defined
            // It could be in a ProjectNode's assignments
            if (unnestSource instanceof ProjectNode projectNode) {
                return extractFromUnnestWithProject(unnestNode, projectNode, context);
            }

            // Or directly in a ValuesNode
            if (unnestSource instanceof ValuesNode valuesNode) {
                return extractFromUnnestDirect(unnestNode, valuesNode);
            }
        }

        return Optional.empty();
    }

    private Optional<List<Expression>> extractFromUnnestWithProject(UnnestNode unnestNode, ProjectNode projectNode, Context context)
    {
        if (unnestNode.getMappings().size() != 1) {
            return Optional.empty();
        }

        UnnestNode.Mapping mapping = unnestNode.getMappings().get(0);
        Symbol arraySymbol = mapping.getInput();

        // Find the array expression in the ProjectNode's assignments
        Expression arrayExpression = projectNode.getAssignments().get(arraySymbol);
        if (arrayExpression == null) {
            return Optional.empty();
        }

        // Check if it's an array constant
        if (!(arrayExpression instanceof Constant arrayConstant)) {
            return Optional.empty();
        }

        return extractConstantsFromArray(arrayConstant);
    }

    private Optional<List<Expression>> extractFromValues(ValuesNode valuesNode)
    {
        // Only handle single-column VALUES
        if (valuesNode.getOutputSymbols().size() != 1) {
            return Optional.empty();
        }

        if (valuesNode.getRows().isEmpty()) {
            return Optional.empty();
        }

        List<Expression> constants = new ArrayList<>();
        for (Expression rowExpression : valuesNode.getRows().get()) {
            // For single-column VALUES, each row is just the value itself (not wrapped in Row)
            if (!(rowExpression instanceof Constant)) {
                return Optional.empty();
            }
            constants.add(rowExpression);
        }

        return Optional.of(constants);
    }

    private Optional<List<Expression>> extractFromUnnestDirect(UnnestNode unnestNode, ValuesNode valuesNode)
    {
        // This handles: UNNEST(ARRAY[1, 2, 3]) where the array is in ValuesNode rows

        if (unnestNode.getMappings().size() != 1) {
            return Optional.empty();
        }

        if (valuesNode.getRows().isEmpty()) {
            return Optional.empty();
        }

        List<Expression> row = valuesNode.getRows().get();

        if (row.size() != 1) {
            return Optional.empty();
        }

        Expression arrayExpression = row.get(0);

        if (!(arrayExpression instanceof Constant arrayConstant)) {
            return Optional.empty();
        }

        return extractConstantsFromArray(arrayConstant);
    }

    private Optional<List<Expression>> extractConstantsFromArray(Constant arrayConstant)
    {
        Object arrayValue = arrayConstant.value();
        if (arrayValue == null) {
            return Optional.empty();
        }

        if (!(arrayValue instanceof io.trino.spi.block.Block block)) {
            return Optional.empty();
        }

        List<Expression> constants = new ArrayList<>();
        io.trino.spi.type.Type elementType = ((io.trino.spi.type.ArrayType) arrayConstant.type()).getElementType();

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                // Preserve NULLs in the constant list for correct IN semantics
                constants.add(new Constant(elementType, null));
            }
            else {
                // Read the value using the proper method for the internal representation
                // Match the Type's getJavaType() to the appropriate getter method
                Object value;
                Class<?> javaType = elementType.getJavaType();
                if (javaType == long.class) {
                    value = elementType.getLong(block, i);
                }
                else if (javaType == double.class) {
                    value = elementType.getDouble(block, i);
                }
                else if (javaType == boolean.class) {
                    value = elementType.getBoolean(block, i);
                }
                else {
                    value = elementType.getObject(block, i);
                }
                constants.add(new Constant(elementType, value));
            }
        }

        return Optional.of(constants);
    }
}
