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

import com.google.common.base.Supplier;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class MergeSortedGrouper<KeyType> implements Grouper<KeyType>
{
  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int keySize;
  private final int recordSize; // size of key + all aggregated values
  private final BufferComparator comparator;

  private ByteBuffer prevKeyBuffer;
  private ByteBuffer recordsBuffer;
  private boolean initialized;
  private int curWriteIndex;

  public MergeSortedGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.keySerde = keySerde;
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];

    keySize = keySerde.keySize();
    int offset = keySize;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    recordSize = keySize + offset;
    comparator = keySerde.bufferComparator();
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get();

      buffer.position(0);
      buffer.limit(keySize);
      prevKeyBuffer = buffer.slice();

      buffer.position(keySize);
      buffer.limit(buffer.capacity());
      recordsBuffer = buffer.slice();

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
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (keyBuffer == null) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.DICTIONARY_FULL;
    }

    if (keyBuffer.remaining() != keySize) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          keyBuffer.remaining(),
          keySize
      );
    }

    if (comparator.compare(keyBuffer, prevKeyBuffer, 0, 0) != 0) {

    }

    return null;
  }



  private void initNewSlot()
  {
    curWriteIndex++;
    final int baseOffset = curWriteIndex * recordSize + keySize;
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(recordsBuffer, baseOffset + aggregatorOffsets[i]);
    }
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < prevKeyBuffer.capacity(); i++) {
      prevKeyBuffer.put(i, (byte) 0);
    }
    curWriteIndex = 0;
  }

  @Override
  public void close()
  {

  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    return null;
  }
}
