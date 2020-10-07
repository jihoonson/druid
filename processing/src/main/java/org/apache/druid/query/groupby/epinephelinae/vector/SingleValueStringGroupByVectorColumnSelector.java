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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.PerSegmentEncodedResultRow;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

public class SingleValueStringGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private final SingleValueDimensionVectorSelector selector;
  private final boolean encodeStrings;

  SingleValueStringGroupByVectorColumnSelector(final SingleValueDimensionVectorSelector selector, boolean encodeStrings)
  {
    this.selector = selector;
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
  public void writeKeyToResultRow(
      final Memory keyMemory,
      final int keyOffset,
      final PerSegmentEncodedResultRow resultRow,
      final int resultRowPosition,
      final int segmentId
  )
  {
    final int id = keyMemory.getInt(keyOffset);
    if (encodeStrings) {
      resultRow.set(resultRowPosition, segmentId, id);
    } else {
      resultRow.set(resultRowPosition, segmentId, selector.lookupName(id));
    }
  }
}
