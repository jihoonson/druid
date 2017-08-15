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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: streaming grouper
public class MergeSortedGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(MergeSortedGrouper.class);

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int keySize;
  private final int recordSize; // size of key + all aggregated values

  private List<AtomicBoolean> validFlags;
  private ByteBuffer buffer;
  private boolean initialized;
  private int curWriteIndex;
  private boolean finished;

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
  }

  @Override
  public void init()
  {
    if (!initialized) {
      buffer = bufferSupplier.get();
      validFlags = new ArrayList<>((buffer.capacity() - keySize) / recordSize);

      reset();
      initialized = true;
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  private boolean keyEquals(ByteBuffer curKeyBuffer, ByteBuffer buffer, int bufferOffset)
  {
    for (int i = 0; i < keySize; i++) {
      if (curKeyBuffer.get(i) != buffer.get(bufferOffset + i)) {
        return false;
      }
    }
    return true;
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

    final int prevRecordOffset = curWriteIndex * recordSize;
    if (curWriteIndex == -1 || !keyEquals(keyBuffer, buffer, prevRecordOffset)) {
      initNewSlot(keyBuffer);
    }

    final int baseOffset = curWriteIndex * recordSize + keySize;
    for (int i = 0; i < aggregatorOffsets.length; i++) {
      aggregators[i].aggregate(buffer, baseOffset + aggregatorOffsets[i]);
    }

    return AggregateResult.ok();
  }

  private void initNewSlot(ByteBuffer newKey)
  {
    if (curWriteIndex >= 0) {
      validFlags.get(curWriteIndex).set(true);
    }
    curWriteIndex++; // TODO: circular
    final int recordOffset = recordSize * curWriteIndex;
    buffer.position(recordOffset);
    buffer.put(newKey);

    final int baseOffset = recordOffset + keySize;
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(buffer, baseOffset + aggregatorOffsets[i]);
    }
    if (validFlags.size() == curWriteIndex) {
      validFlags.add(new AtomicBoolean(false));
    } else if (validFlags.size() > curWriteIndex) {
      validFlags.get(curWriteIndex).set(false);
    } else {
      throw new ISE("WTF? validFlags.size[%d] is much smaller than curWriteIndex[%d]?", validFlags.size(), curWriteIndex);
    }
  }

  @Override
  public void reset()
  {
    for (AtomicBoolean flag : validFlags) {
      flag.set(false);
    }
    curWriteIndex = -1;
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

  public void finish()
  {
    finished = true;
    if (curWriteIndex >= 0) {
      validFlags.get(curWriteIndex).set(true);
    }
    curWriteIndex++;
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    return new Iterator<Entry<KeyType>>()
    {
      int curReadIndex = 0;

      {
        curReadIndex = findNext(0);
      }

      private int findNext(int curReadIndex)
      {
        for (int i = curReadIndex; i < validFlags.size(); i++) {
          if (validFlags.get(i).get()) {
            return i;
          }
        }
        return curWriteIndex;
      }

      @Override
      public boolean hasNext()
      {
        return !finished || (curReadIndex < curWriteIndex);
      }

      @Override
      public Entry<KeyType> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int recordOffset = recordSize * curReadIndex;
        final KeyType key = keySerde.fromByteBuffer(buffer, recordOffset);

        final int baseOffset = recordOffset + keySize;
        final Object[] values = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
          values[i] = aggregators[i].get(buffer, baseOffset + aggregatorOffsets[i]);
        }
        curReadIndex = findNext(curReadIndex + 1);

        return new Entry<>(key, values);
      }
    };
  }
}
