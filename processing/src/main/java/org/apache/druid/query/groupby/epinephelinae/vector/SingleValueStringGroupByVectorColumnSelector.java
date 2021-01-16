/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.groupby.epinephelinae.vector;

import com.google.common.base.Supplier;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.groupby.PerSegmentEncodedResultRow;
import org.apache.druid.query.groupby.epinephelinae.MergedDictionary;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper.MemoryComparator;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

public class SingleValueStringGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private final SingleValueDimensionVectorSelector selector; // TODO: maybe can add isDictionaryEncoded to the selector?
  private final ColumnCapabilities columnCapabilities;
  private final boolean encodeStrings; // encodeString when deserialize to resultRow

  SingleValueStringGroupByVectorColumnSelector(
      final SingleValueDimensionVectorSelector selector,
      final ColumnCapabilities columnCapabilities,
      final boolean encodeStrings
  )
  {
    this.selector = selector;
    this.columnCapabilities = columnCapabilities;
    this.encodeStrings = encodeStrings;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  @Override
  public void writeKeys(
      final WritableMemory keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    final int[] vector = selector.getRowVector();

    if (keySize == Integer.BYTES) {
      keySpace.putIntArray(keyOffset, vector, startRow, endRow - startRow);
    } else {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putInt(j, vector[i]);
      }
    }
  }

  @Override
  public void convertKeys(
      final Supplier<MergedDictionary> mergedDictionariesSupplier,
      final int segmentId,
      final WritableMemory keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    assert encodeStrings;
    if (encodeStrings && StringGroupByColumnSelectorStrategy.canUseDictionary(columnCapabilities)) {
      assert segmentId > -1;
      // convert dictionaries
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        final int oldDictId = keySpace.getInt(j);
        final int newDictId = mergedDictionariesSupplier.get().getNewDictId(
            segmentId,
            oldDictId
        );
        keySpace.putInt(j, newDictId);
      }
    } else {
      // do nothing
    }
  }

  @Override
  public void writeKeyToResultRow(
      final Memory keyMemory,
      final int keyOffset,
      final PerSegmentEncodedResultRow resultRow,
      final int resultRowPosition,
      final int segmentId
      // TODO: maybe decodeStrings should be added to here?
  )
  {
    final int id = keyMemory.getInt(keyOffset);
    if (encodeStrings && StringGroupByColumnSelectorStrategy.canUseDictionary(columnCapabilities)) {
      resultRow.set(resultRowPosition, segmentId, id);
    } else {
      resultRow.set(resultRowPosition, segmentId, selector.lookupName(id));
    }
  }

  @Override
  public MemoryComparator memoryComparator(int keyOffset, @Nullable StringComparator stringComparator)
  {
    final boolean canCompareInts = StringGroupByColumnSelectorStrategy.canUseDictionary(columnCapabilities);
    final StringComparator comparator = stringComparator == null ? StringComparators.LEXICOGRAPHIC : stringComparator;
    if (canCompareInts && StringComparators.LEXICOGRAPHIC.equals(comparator)) {
      return (lhs, rhs, lhsOffset, rhsOffset) -> Integer.compare(
          lhs.getInt(lhsOffset + keyOffset),
          rhs.getInt(rhsOffset + keyOffset)
      );
    } else {
      throw new UnsupportedOperationException("not implemented");
    }
  }
}
