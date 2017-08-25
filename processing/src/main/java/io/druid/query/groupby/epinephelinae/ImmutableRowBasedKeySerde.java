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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.DoubleRowBasedKeySerdeHelper;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.FloatRowBasedKeySerdeHelper;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.LimitPushDownStringRowBasedKeySerdeHelper;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.LongRowBasedKeySerdeHelper;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.RowBasedKeySerdeHelper;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class ImmutableRowBasedKeySerde extends AbstractRowBasedKeySerde
{
  private static final int UNKNOWN_STRING_ID = -1;

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
    super(includeTimestamp, sortByDimsFirst, dimensions, limitSpec, valueTypes, dictionary, createReverseDictionary(dictionary), keyBuffer);
    initializeSortableIds();
  }

  private static Object2IntMap<String> createReverseDictionary(List<String> dictionary)
  {
    final Object2IntMap<String> reverseDictionary = new Object2IntArrayMap<>();
    reverseDictionary.defaultReturnValue(UNKNOWN_STRING_ID);
    for (int i = 0; i < dictionary.size(); i++) {
      reverseDictionary.put(dictionary.get(i), i);
    }
    return reverseDictionary;
  }

  @Override
  RowBasedKeySerdeHelper makeRowBasedKeySerdeHelper(
      ValueType valueType, int keyBufferPosition, boolean pushDownLimit, @Nullable StringComparator stringComparator
  )
  {
    if (pushDownLimit) {
      switch (valueType) {
        case STRING:
          return new StringRowBasedKeySerdeHelper(keyBufferPosition);
        case LONG:
          return new LongRowBasedKeySerdeHelper(keyBuffer, keyBufferPosition);
        case FLOAT:
          return new FloatRowBasedKeySerdeHelper(keyBuffer, keyBufferPosition);
        case DOUBLE:
          return new DoubleRowBasedKeySerdeHelper(keyBuffer, keyBufferPosition);
        default:
          throw new IAE("invalid type: %s", valueType);
      }
    } else {
      final boolean isNumericComparator = stringComparator == StringComparators.NUMERIC;
      switch (valueType) {
        case STRING:
          if (stringComparator == null) {
            stringComparator = StringComparators.LEXICOGRAPHIC;
          }
          return new LimitPushDownStringRowBasedKeySerdeHelper(keyBufferPosition, stringComparator);
        case LONG:
          if (stringComparator == null || isNumericComparator) {
            return new LongRowBasedKeySerdeHelper(keyBuffer, keyBufferPosition);
          } else {
            return new
          }
      }
    }
    return null;
  }

  @Override
  void checkSortableIds()
  {
    Preconditions.checkState(sortableIds != null, "sortableIds must not be null");
  }

  @Override
  public void reset()
  {
    // do nothing
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
      final int id = reverseDictionary.getInt(s);
      if (id == UNKNOWN_STRING_ID) {
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
