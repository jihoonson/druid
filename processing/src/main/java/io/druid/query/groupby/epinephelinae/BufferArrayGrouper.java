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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BufferArrayGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(BufferArrayGrouper.class);

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int[] keyOffsets;
  private final boolean[] keyCheck; // TODO: rename
  private final int cardinality;
  private final int numBytesPerAggValues;

  private boolean initialized = false;
  private ByteBuffer aggBuffer;
  private ByteBuffer keyBuffer;
  private final int keySize;

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
    this.keySize = keySerde.keySize();
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.keyOffsets = new int[cardinality];
    this.keyCheck = new boolean[cardinality];
    this.cardinality = cardinality;

    int offset = 0;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    numBytesPerAggValues = offset;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get();
      final int keyArena = keySize * cardinality;
      final int aggArena = buffer.capacity() - keyArena;
      Preconditions.checkState(keyArena < buffer.capacity(), "Too many keys[%s]", cardinality);
      Preconditions.checkState(
          aggArena >= numBytesPerAggValues * cardinality,
          "Not enough aggregation buffer space to execute this query. Try increasing druid.processing.buffer.sizeBytes."
      );

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
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (!keyCheck[keyHash]) {
      keyCheck[keyHash] = true;
      this.keyBuffer.position(keyHash * keySize);
      this.keyBuffer.put(keyBuffer);
    }

    final int baseOffset = keyOffsets[keyHash];

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(aggBuffer, baseOffset + aggregatorOffsets[i]);
    }

    return AggregateResult.ok();
  }

  @Override
  public AggregateResult aggregate(KeyType key)
  {
    // BufferArrayGrouper is used only for dictionary-indexed single-value string dimensions.
    // Here, the key contains the dictionary-encoded value of the grouping key
    // and we use it as the index for the aggregation array.
    final ByteBuffer fromKey = keySerde.toByteBuffer(key);
    final int keyHash = fromKey.getInt();
    fromKey.rewind();
    return aggregate(key, keyHash);
  }

  @Override
  public void reset()
  {
    for (int i = 0; i < cardinality; i++) {
      keyOffsets[i] = i * numBytesPerAggValues;
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
        LOG.warn(e, "Could not close aggregator [%s], skipping.", aggregator);
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    // TODO: sorted
    return new Iterator<Entry<KeyType>>()
    {
      private static final int NOT_INITIALIZED = -2;
      private static final int NOT_FOUND = -1;
      int cur = NOT_INITIALIZED;

      private int findNextKeyIndex()
      {
        for (int i = cur; i < keyCheck.length; i++) {
          if (keyCheck[i]) {
            return i;
          }
        }
        return NOT_FOUND;
      }

      @Override
      public boolean hasNext()
      {
        if (cur == NOT_INITIALIZED) {
          cur = 0;
        }
        cur = findNextKeyIndex();
        return cur > NOT_FOUND;
      }

      @Override
      public Entry<KeyType> next()
      {
        if (cur == NOT_INITIALIZED) {
          throw new ISE("hasNext() must be called first");
        } else if (cur == NOT_FOUND) {
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
