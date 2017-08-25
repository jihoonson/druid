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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde.RowBasedKeySerdeHelper;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractRowBasedKeySerde implements KeySerde<RowBasedKey>
{
  private final boolean includeTimestamp;
  private final boolean sortByDimsFirst;
  private final List<DimensionSpec> dimensions;
  private final int dimCount;
  private final int keySize;
  private final List<RowBasedKeySerdeHelper> serdeHelpers;
  private final DefaultLimitSpec limitSpec;
  private final List<ValueType> valueTypes;

  protected final List<String> dictionary;
  protected final Object2IntMap<String> reverseDictionary;
  protected final ByteBuffer keyBuffer;

  // dictionary id -> its position if it were sorted by dictionary value
  protected int[] sortableIds = null;

  AbstractRowBasedKeySerde(
      final boolean includeTimestamp,
      final boolean sortByDimsFirst,
      final List<DimensionSpec> dimensions,
      final DefaultLimitSpec limitSpec,
      final List<ValueType> valueTypes,
      final List<String> dictionary,
      final Object2IntMap<String> reverseDictionary,
      final ByteBuffer keyBuffer
  )
  {
    this.includeTimestamp = includeTimestamp;
    this.sortByDimsFirst = sortByDimsFirst;
    this.dimensions = dimensions;
    this.dimCount = dimensions.size();
    this.valueTypes = valueTypes;
    this.limitSpec = limitSpec;
    this.serdeHelpers = makeSerdeHelpers();
    this.keySize = (includeTimestamp ? Long.BYTES : 0) + computeTotalKeySize(serdeHelpers);
    this.dictionary = dictionary;
    this.reverseDictionary = reverseDictionary;
    this.keyBuffer = keyBuffer;
  }

  private List<RowBasedKeySerdeHelper> makeSerdeHelpers()
  {
    List<RowBasedKeySerdeHelper> helpers = new ArrayList<>();
    int keyBufferPosition = 0;
    for (int i = 0; i < dimCount; i++) {
      final StringComparator stringComparator;
      if (limitSpec != null) {
        final String dimName = dimensions.get(i).getOutputName();
        stringComparator = DefaultLimitSpec.getComparatorForDimName(limitSpec, dimName);
      } else {
        stringComparator = null;
      }
      RowBasedKeySerdeHelper helper = makeRowBasedKeySerdeHelper(
          valueTypes.get(i),
          keyBufferPosition,
          stringComparator
      );
      keyBufferPosition += helper.getKeyBufferValueSize();
      helpers.add(helper);
    }
    return helpers;
  }

  abstract RowBasedKeySerdeHelper makeRowBasedKeySerdeHelper(
      ValueType valueType,
      int keyBufferPosition,
      @Nullable StringComparator stringComparator
  );

  // TODO: move to makeSerdeHelpers()?
  private static int computeTotalKeySize(List<RowBasedKeySerdeHelper> serdeHelpers)
  {
    int size = 0;
    for (RowBasedKeySerdeHelper helper : serdeHelpers) {
      size += helper.getKeyBufferValueSize();
    }
    return size;
  }

  @Override
  public int keySize()
  {
    return keySize;
  }

  @Override
  public Class<RowBasedKey> keyClazz()
  {
    return RowBasedKey.class;
  }

  @Override
  public ByteBuffer toByteBuffer(RowBasedKey key)
  {
    keyBuffer.rewind();

    final int dimStart;
    if (includeTimestamp) {
      keyBuffer.putLong((long) key.getKey()[0]);
      dimStart = 1;
    } else {
      dimStart = 0;
    }
    for (int i = dimStart; i < key.getKey().length; i++) {
      if (!serdeHelpers.get(i - dimStart).putToKeyBuffer(key, i)) {
        return null;
      }
    }

    keyBuffer.flip();
    return keyBuffer;
  }

  @Override
  public RowBasedKey fromByteBuffer(ByteBuffer buffer, int position)
  {
    final int dimStart;
    final Comparable[] key;
    final int dimsPosition;

    if (includeTimestamp) {
      key = new Comparable[dimCount + 1];
      key[0] = buffer.getLong(position);
      dimsPosition = position + Longs.BYTES;
      dimStart = 1;
    } else {
      key = new Comparable[dimCount];
      dimsPosition = position;
      dimStart = 0;
    }

    for (int i = dimStart; i < key.length; i++) {
      // Writes value from buffer to key[i]
      serdeHelpers.get(i - dimStart).getFromByteBuffer(buffer, dimsPosition, i, key);
    }

    return new RowBasedKey(key);
  }

  protected void initializeSortableIds()
  {
    final int dictionarySize = dictionary.size();
    final Pair<String, Integer>[] dictAndIds = new Pair[dictionarySize];
    for (int id = 0; id < dictionarySize; id++) {
      dictAndIds[id] = new Pair<>(dictionary.get(id), id);
    }
    Arrays.sort(dictAndIds, Comparator.comparing(pair -> pair.lhs));

    sortableIds = new int[dictionarySize];
    for (int i = 0; i < dictionarySize; i++) {
      sortableIds[dictAndIds[i].rhs] = i;
    }
  }

  abstract void checkSortableIds();

  @Override
  public BufferComparator bufferComparator()
  {
    checkSortableIds();

    if (includeTimestamp) {
      if (sortByDimsFirst) {
        return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
          final int cmp = RowBasedGrouperHelper.compareDimsInBuffersForNullFudgeTimestamp(
              serdeHelpers,
              dimCount,
              lhsBuffer,
              rhsBuffer,
              lhsPosition,
              rhsPosition
          );
          if (cmp != 0) {
            return cmp;
          }

          return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
        };
      } else {
        return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
          final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

          if (timeCompare != 0) {
            return timeCompare;
          }

          return RowBasedGrouperHelper.compareDimsInBuffersForNullFudgeTimestamp(
              serdeHelpers,
              dimCount,
              lhsBuffer,
              rhsBuffer,
              lhsPosition,
              rhsPosition
          );
        };
      }
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        for (int i = 0; i < dimCount; i++) {
          final int cmp = serdeHelpers.get(i).compare(
              lhsBuffer,
              rhsBuffer,
              lhsPosition,
              rhsPosition
          );

          if (cmp != 0) {
            return cmp;
          }
        }

        return 0;
      };
    }
  }

  @Override
  public BufferComparator bufferComparatorWithAggregators(
      AggregatorFactory[] aggregatorFactories, int[] aggregatorOffsets
  )
  {
    final List<RowBasedKeySerdeHelper> adjustedSerdeHelpers;
    final List<Boolean> needsReverses = Lists.newArrayList();
    List<RowBasedKeySerdeHelper> orderByHelpers = new ArrayList<>();
    List<RowBasedKeySerdeHelper> otherDimHelpers = new ArrayList<>();
    Set<Integer> orderByIndices = new HashSet<>();

    int aggCount = 0;
    boolean needsReverse;
    for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
      needsReverse = orderSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING;
      int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
      if (dimIndex >= 0) {
        RowBasedKeySerdeHelper serdeHelper = serdeHelpers.get(dimIndex);
        orderByHelpers.add(serdeHelper);
        orderByIndices.add(dimIndex);
        needsReverses.add(needsReverse);
      } else {
        int aggIndex = OrderByColumnSpec.getAggIndexForOrderBy(orderSpec, Arrays.asList(aggregatorFactories));
        if (aggIndex >= 0) {
          final RowBasedKeySerdeHelper serdeHelper;
          final StringComparator cmp = orderSpec.getDimensionComparator();
          final int aggOffset = aggregatorOffsets[aggIndex] - Ints.BYTES;
          final ValueType valueType = ValueType.fromString(aggregatorFactories[aggIndex].getTypeName());

          if (!ValueType.isNumeric(valueType)) {
            throw new IAE("Cannot order by a non-numeric aggregator[%s]", orderSpec);
          }

          serdeHelper = makeRowBasedKeySerdeHelper(valueType, aggOffset, cmp);

          orderByHelpers.add(serdeHelper);
          needsReverses.add(needsReverse);
          aggCount++;
        }
      }
    }

    for (int i = 0; i < dimCount; i++) {
      if (!orderByIndices.contains(i)) {
        otherDimHelpers.add(serdeHelpers.get(i));
        needsReverses.add(false); // default to Ascending order if dim is not in an orderby spec
      }
    }

    adjustedSerdeHelpers = orderByHelpers;
    adjustedSerdeHelpers.addAll(otherDimHelpers);

    final int fieldCount = dimCount + aggCount;

    if (includeTimestamp) {
      if (sortByDimsFirst) {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int cmp = RowBasedGrouperHelper.compareDimsInBuffersForNullFudgeTimestampForPushDown(
                adjustedSerdeHelpers,
                needsReverses,
                fieldCount,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );
            if (cmp != 0) {
              return cmp;
            }

            return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
          }
        };
      } else {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

            if (timeCompare != 0) {
              return timeCompare;
            }

            int cmp = RowBasedGrouperHelper.compareDimsInBuffersForNullFudgeTimestampForPushDown(
                adjustedSerdeHelpers,
                needsReverses,
                fieldCount,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );

            return cmp;
          }
        };
      }
    } else {
      return new Grouper.BufferComparator()
      {
        @Override
        public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
        {
          for (int i = 0; i < fieldCount; i++) {
            final int cmp;
            if (needsReverses.get(i)) {
              cmp = adjustedSerdeHelpers.get(i).compare(
                  rhsBuffer,
                  lhsBuffer,
                  rhsPosition,
                  lhsPosition
              );
            } else {
              cmp = adjustedSerdeHelpers.get(i).compare(
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );
            }

            if (cmp != 0) {
              return cmp;
            }
          }

          return 0;
        }
      };
    }
  }
}
