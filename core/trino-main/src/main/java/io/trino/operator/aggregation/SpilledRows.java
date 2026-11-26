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
package io.trino.operator.aggregation;

import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;

/**
 * Spill state of aggregations that buffer their input rows (DISTINCT and ORDER BY):
 * the rows of a group, as an array of rows.
 */
final class SpilledRows
{
    private SpilledRows() {}

    public static Type spillType(List<Type> rowTypes)
    {
        return new ArrayType(RowType.anonymous(rowTypes));
    }

    public static void writeRows(BlockBuilder output, int rowCount, RowWriter rowWriter)
    {
        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            RowBlockBuilder rowBuilder = (RowBlockBuilder) elementBuilder;
            for (int row = 0; row < rowCount; row++) {
                int currentRow = row;
                rowBuilder.buildEntry(fieldBuilders -> rowWriter.write(currentRow, fieldBuilders));
            }
        });
    }

    /**
     * Unnests the spilled rows of each position of {@code block}, which belongs to group {@code groupIds[position]}.
     */
    public static Rows readRows(int[] groupIds, Block block)
    {
        ColumnarArray arrays = toColumnarArray(block);
        Block elements = arrays.getElementsBlock();
        int rowCount = elements.getPositionCount();
        int[] rowGroupIds = new int[rowCount];
        for (int position = 0; position < arrays.getPositionCount(); position++) {
            int offset = arrays.getOffset(position);
            Arrays.fill(rowGroupIds, offset, offset + arrays.getLength(position), groupIds[position]);
        }
        Page page = new Page(rowCount, getRowFieldsFromBlock(elements).toArray(Block[]::new));
        return new Rows(rowGroupIds, page);
    }

    public record Rows(int[] groupIds, Page page) {}

    public interface RowWriter
    {
        void write(int row, List<BlockBuilder> fieldBuilders);
    }
}
