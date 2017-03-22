/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.join;

import io.druid.data.input.Row;
import io.druid.query.join.AnnotatedJoinSpec.InputDirection;
import it.unimi.dsi.fastutil.ints.IntList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HashJoin
{
//  private final ByteBuffer outBuffer;
  private final AnnotatedJoinSpec joinSpec;
  private final List<List<List<Object>>> outBuffer;
  private final JoinHashTable hashTable;
  private int writtenSize;

  public HashJoin(
//      ByteBuffer outBuffer,
      AnnotatedJoinSpec joinSpec,
      JoinHashTable hashTable
  )
  {
    this.joinSpec = joinSpec;
    this.outBuffer = new ArrayList<>();
    this.hashTable = hashTable;
  }

  /**
   * Output size
   *
   * @return
   */
  public int size()
  {
    return writtenSize;
  }

  public void add(Row row)
  {

  }

  public void add(List<List<Object>> key, List<List<Object>> allValues)
  {
    final List<List<Object>> matched = hashTable.match(key);

    if (matched != null) {
      final List<List<Object>> outVal = new ArrayList<>();

      final List<InputDirection> directions = joinSpec.getOutputDirections();
      final IntList dimIndexes = joinSpec.getOutputDimIndexes();

      for (int i = 0; i < dimIndexes.size(); i++) {
        if (directions.get(i) == AnnotatedJoinSpec.InputDirection.RIGHT) {
          outVal.add(allValues.get(dimIndexes.getInt(i)));
        } else {
          outVal.add(matched.get(dimIndexes.getInt(i)));
        }
      }

      outBuffer.add(outVal);
    }
  }

  /**
   *
   * @param buffer buffer containing previous join result
   */
  public void add(ByteBuffer buffer, int position /*, how to extract key and payload? */)
  {

  }

  /**
   * Return a sequence which iterates join result
   *
   * @return
   */
  public Iterator<List<List<Object>>> getIterator()
  {
    return outBuffer.iterator();
  }
}
