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
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A grouper for array-based aggregation.  The array consists of records.  The first record is to store
 * {@link StringGroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}.
 * The memory format of the record is like below.
 *
 * +---------------+----------+--------------------+--------------------+-----+
 * | used flag (4) |  key (4) | agg_1 (fixed size) | agg_2 (fixed size) | ... |
 * +---------------+----------+--------------------+--------------------+-----+
 */
public class BufferArrayGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(BufferArrayGrouper.class);
  private static final int USED_FLAG_SIZE = Ints.BYTES;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int cardinality;
  private final int recordSize; // keySize + size of all aggregated values

  private boolean initialized = false;
  private ByteBuffer keyBuffer;
  private ByteBuffer valBuffer;

  static <KeyType> int keySize(KeySerde<KeyType> keySerde)
  {
    return keySerde.keySize() + USED_FLAG_SIZE;
  }

  public BufferArrayGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int cardinality
  )
  {
    Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories");
    Preconditions.checkArgument(cardinality > 0, "Cardinality must a non-zero positive number");

    this.bufferSupplier = Preconditions.checkNotNull(bufferSupplier, "bufferSupplier");
    this.keySerde = Preconditions.checkNotNull(keySerde, "keySerde");
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.cardinality = cardinality;

    int offset = 0;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    recordSize = USED_FLAG_SIZE + offset;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get().duplicate();

      buffer.position(0);
      buffer.limit(keySerde.keySize());
      keyBuffer = buffer.slice();

      buffer.position(keySerde.keySize());
      buffer.limit(buffer.capacity());
      valBuffer = buffer.slice();

      reset();

      initialized = true;
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(KeyType key, int dimIndex)
  {
    Preconditions.checkArgument(dimIndex > -1, "Invalid dimIndex[%s]", dimIndex);

    final ByteBuffer fromKey = keySerde.toByteBuffer(key);
    if (fromKey == null) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.DICTIONARY_FULL;
    }

    if (fromKey.remaining() != keySerde.keySize()) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          fromKey.remaining(),
          keySerde.keySize()
      );
    }

    final int recordOffset = dimIndex * recordSize;
    final int baseOffset = recordOffset + USED_FLAG_SIZE;

    if (recordOffset + recordSize > valBuffer.capacity()) {
      // This error cannot be recoverd, and the query must fail
      throw new ISE(
          "A record of size [%d] cannot be written to the array buffer at offset[%d] "
          + "because it exceeds the buffer capacity[%d]. Try increasing druid.processing.buffer.sizeBytes",
          recordSize,
          recordOffset,
          valBuffer.capacity()
      );
    }

    if (!isUsedKey(recordOffset)) {
      initializeSlot(dimIndex);
    }

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(valBuffer, baseOffset + aggregatorOffsets[i]);
    }

    return AggregateResult.ok();
  }

  private void initializeSlot(int dimIndex)
  {
    final int recordOffset = dimIndex * recordSize;
    final int baseOffset = recordOffset + USED_FLAG_SIZE;
    valBuffer.position(recordOffset);
    valBuffer.putInt(Groupers.getUsedFlag(dimIndex));
    for (int j = 0; j < aggregators.length; ++j) {
      aggregators[j].init(valBuffer, baseOffset + aggregatorOffsets[j]);
    }
  }

  @Override
  public AggregateResult aggregate(KeyType key)
  {
    // BufferArrayGrouper is used only for dictionary-indexed single-value string dimensions.
    // Here, the key contains the dictionary-encoded value of the grouping key
    // and we use it as the index for the aggregation array.
    final ByteBuffer fromKey = keySerde.toByteBuffer(key);
    final int dimIndex = fromKey.getInt() + 1; // the first index is for missing value
    fromKey.rewind();
    return aggregate(key, dimIndex);
  }

  private boolean isUsedKey(int pos)
  {
    return (valBuffer.get(pos) & 0x80) == 0x80;
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < cardinality + 1; i++) { // for missing value
      valBuffer.putInt(i * recordSize, 0);
    }
  }

  @Override
  public void close()
  {
    for (BufferAggregator aggregator : aggregators) {
      try {
        aggregator.close();
      }
      catch (Exception e) {
        LOG.warn(e, "Could not close aggregator [%s], skipping.", aggregator);
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    return sorted ? sortedIterator() : plainIterator();
  }

  private Iterator<Entry<KeyType>> sortedIterator()
  {
    // Sorted iterator is currently not used because there is no way to get grouping key's cardinality when merging
    // partial aggregation result in brokers and even data nodes (historicals and realtimes).
    // However, it should be used in the future.
    final BufferComparator comparator = keySerde.bufferComparator();
    final List<Integer> wrappedOffsets = IntStream.range(0, cardinality + 1).boxed().collect(Collectors.toList());
    wrappedOffsets.sort(
        (lhs, rhs) -> comparator.compare(valBuffer, valBuffer, lhs + USED_FLAG_SIZE, rhs + USED_FLAG_SIZE)
    );

    return new ResultIterator(wrappedOffsets);
  }

  private Iterator<Entry<KeyType>> plainIterator()
  {
    return new ResultIterator(
        IntStream.range(0, cardinality + 1).boxed().collect(Collectors.toList())
    );
  }

  private class ResultIterator implements Iterator<Entry<KeyType>>
  {
    private static final int NOT_FOUND = -1;

    private final Iterator<Integer> keyIndexIterator;
    private int cur;
    private boolean needFindNext;

    ResultIterator(Collection<Integer> keyOffsets)
    {
      keyIndexIterator = keyOffsets.iterator();
      cur = NOT_FOUND;
      needFindNext = true;
    }

    private int findNextKeyIndex()
    {
      while (keyIndexIterator.hasNext()) {
        final int index = keyIndexIterator.next();
        if (isUsedKey(index * recordSize)) {
          return index;
        }
      }
      return NOT_FOUND;
    }

    @Override
    public boolean hasNext()
    {
      if (needFindNext) {
        cur = findNextKeyIndex();
        needFindNext = false;
      }
      return cur > NOT_FOUND;
    }

    @Override
    public Entry<KeyType> next()
    {
      if (cur == NOT_FOUND) {
        throw new NoSuchElementException();
      }

      keyBuffer.putInt(0, cur - 1);

      needFindNext = true;
      final int baseOffset = cur * recordSize + USED_FLAG_SIZE;
      final Object[] values = new Object[aggregators.length];
      for (int i = 0; i < aggregators.length; i++) {
        values[i] = aggregators[i].get(valBuffer, baseOffset + aggregatorOffsets[i]);
      }
      return new Entry<>(keySerde.fromByteBuffer(keyBuffer, 0), values);
    }
  }
}
