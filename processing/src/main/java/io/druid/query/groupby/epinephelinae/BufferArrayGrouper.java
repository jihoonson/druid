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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BufferArrayGrouper<KeyType> implements Grouper<KeyType>
{
  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int[] keyOffsets;
  private final int cardinality;
  private final int numBytesPerRecord;

  private boolean initialized = false;
  private ByteBuffer buffer;
  private ByteBuffer aggBuffer;
  private ByteBuffer keyBuffer;
  private final int keySize;
  private int keyArena;

  private final boolean[] keyFilter;

  public BufferArrayGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int cardinality
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.keyOffsets = new int[cardinality];
    this.cardinality = cardinality;

    int offset = 0;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    numBytesPerRecord = offset;
    keyFilter = new boolean[cardinality];
  }

  @Override
  public void init()
  {
    if (!initialized) {
      buffer = bufferSupplier.get();
      keyArena = buffer.capacity() / 2;

      keyBuffer = buffer.duplicate();
      keyBuffer.position(0);
      keyBuffer.limit(keyArena);
      keyBuffer = keyBuffer.slice();

      aggBuffer = buffer.duplicate();
      aggBuffer.position(keyArena);
      aggBuffer.limit(buffer.capacity());
      aggBuffer = aggBuffer.slice();

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
    return aggregate(key);
  }

  @Override
  public AggregateResult aggregate(Object obj, KeyType key)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    final IndexedInts indexedInts = (IndexedInts) obj;
    final int dimIndex = indexedInts.get(0);
    if (!keyFilter[dimIndex]) {
      keyFilter[dimIndex] = true;
      this.keyBuffer.position(dimIndex * keySize);
      this.keyBuffer.put(keyBuffer);
    }

    final int baseOffset = keyOffsets[dimIndex];

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(aggBuffer, baseOffset + aggregatorOffsets[i]);
    }

    return AggregateResult.ok();
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < cardinality; i++) {
      keyOffsets[i] = i * numBytesPerRecord;
      final int baseOffset = keyOffsets[i];
      for (int j = 0; j < aggregators.length; ++j) {
        aggregators[j].init(aggBuffer, baseOffset + aggregatorOffsets[j]);
      }
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
        throw e;
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    return new Iterator<Entry<KeyType>>()
    {
      int cur = 0;

      @Override
      public boolean hasNext()
      {
        return cur < cardinality;
      }

      @Override
      public Entry<KeyType> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final int baseOffset = keyOffsets[cur];
        final Object[] values = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
          values[i] = aggregators[i].get(aggBuffer, baseOffset + aggregatorOffsets[i]);
        }
        final Entry<KeyType> entry = new Entry<>(keySerde.fromByteBuffer(keyBuffer, cur * keySize), values);
        cur++;
        return entry;
      }
    };
  }
}
