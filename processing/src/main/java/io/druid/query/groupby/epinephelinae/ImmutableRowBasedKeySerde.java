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

package io.druid.query.groupby.epinephelinae;

import com.google.common.primitives.Ints;
import io.druid.java.util.common.ISE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.List;

// TODO: add a new super class
public class ImmutableRowBasedKeySerde extends RowBasedKeySerde
{
  public ImmutableRowBasedKeySerde(
      final boolean includeTimestamp,
      final boolean sortByDimsFirst,
      final List<DimensionSpec> dimensions,
      final DefaultLimitSpec limitSpec,
      final List<ValueType> valueTypes,
      List<String> dictionary,
      ByteBuffer keyBuffer
  )
  {
    super(includeTimestamp, sortByDimsFirst, dimensions, -1, limitSpec, valueTypes, dictionary);
    this.keyBuffer = keyBuffer;
    initializeSortableIds();
  }

  @Override
  protected RowBasedKeySerdeHelper makeStringRowBasedKeySerdeHelper(int keyBufferPosition)
  {
    return new StringRowBasedKeySerdeHelper(keyBufferPosition);
  }

  private class StringRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
  {
    final int keyBufferPosition;

    public StringRowBasedKeySerdeHelper(int keyBufferPosition)
    {
      this.keyBufferPosition = keyBufferPosition;
    }

    @Override
    public int getKeyBufferValueSize()
    {
      return Ints.BYTES;
    }

    @Override
    public boolean putToKeyBuffer(RowBasedKey key, int idx)
    {
      final String s = (String) key.getKey()[idx];
      final Integer id = reverseDictionary.get(s);
      if (id == null) {
        // TODO: throw a proper exception
        throw new ISE("Cannot find an id for key[%s]", s);
      }
      keyBuffer.putInt(id);
      return true;
    }

    @Override
    public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
    {
      dimValues[dimValIdx] = dictionary.get(buffer.getInt(initialOffset + keyBufferPosition));
    }

    @Override
    public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
    {
      return Ints.compare(
          sortableIds[lhsBuffer.getInt(lhsPosition + keyBufferPosition)],
          sortableIds[rhsBuffer.getInt(rhsPosition + keyBufferPosition)]
      );
    }
  }
}
