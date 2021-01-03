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

package org.apache.druid.query.groupby.epinephelinae.collection;

import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

public class MemoryOpenHashTable2
{
  private final WritableMemory flagMemory;
  private final WritableMemory bucketMemory;
  private final int keySize;
  private final int valueSize;
  private final int bucketSize;

  // Maximum number of elements in the table (based on numBuckets and maxLoadFactor).
  private final int maxSize;

  // Number of available/used buckets in the table. Always a power of two.
  private final int numBuckets;

  // Mask that clips a number to [0, numBuckets). Used when searching through buckets.
  private final int bucketMask;

  // Number of elements in the table right now.
  private int size;

  public MemoryOpenHashTable2(
      final WritableMemory tableMemory,
      final int numBuckets,
      final int maxSize,
      final int keySize,
      final int valueSize
  )
  {
    this.numBuckets = numBuckets;
    this.bucketMask = numBuckets - 1;
    this.maxSize = maxSize;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.bucketSize = bucketSize(keySize, valueSize);

    final long numFlagContainers = (long) Math.ceil(numBuckets / (double) Byte.SIZE);
    flagMemory = tableMemory.writableRegion(0L, numFlagContainers);
    bucketMemory = tableMemory.writableRegion(numFlagContainers, tableMemory.getCapacity() - numFlagContainers);

    // Our main intended users (VectorGrouper implementations) need the tableMemory to be backed by a big-endian
    // ByteBuffer that is coterminous with the tableMemory, since it's going to feed that buffer into VectorAggregators
    // instead of interacting with our WritableMemory directly. Nothing about this class actually requires that the
    // Memory be backed by a ByteBuffer, but we'll check it here anyway for the benefit of our biggest customer.
    verifyMemoryIsByteBuffer(tableMemory);

    if (!flagMemory.getTypeByteOrder().equals(ByteOrder.nativeOrder())) {
      throw new ISE("flagMemory must be native byte order");
    }

    if (!bucketMemory.getTypeByteOrder().equals(ByteOrder.nativeOrder())) {
      throw new ISE("bucketMemory must be native byte order");
    }

    if (tableMemory.getCapacity() != memoryNeeded(numBuckets, bucketSize)) {
      throw new ISE(
          "tableMemory must be size[%,d] but was[%,d]",
          memoryNeeded(numBuckets, bucketSize),
          tableMemory.getCapacity()
      );
    }

    if (maxSize >= numBuckets) {
      throw new ISE("maxSize must be less than numBuckets");
    }

    if (Integer.bitCount(numBuckets) != 1) {
      throw new ISE("numBuckets must be a power of two but was[%,d]", numBuckets);
    }

    clear();
  }

  public void clear()
  {
    size = 0;

    // Clear used flags.
//    int container;
//    for (container = 0; container < flagMemory.getCapacity(); container += Integer.BYTES) {
//      flagMemory.putInt(container, 0);
//    }
//
//    if (container != flagMemory.getCapacity()) {
//      for (container = container - Integer.BYTES; container < flagMemory.getCapacity(); container++) {
//        flagMemory.putByte(container, (byte) 0);
//      }
//    }
    flagMemory.clear();
  }

  public static int bucketSize(final int keySize, final int valueSize)
  {
    return keySize + valueSize;
  }

  private static void verifyMemoryIsByteBuffer(final Memory memory)
  {
    final ByteBuffer buffer = memory.getByteBuffer();

    if (buffer == null) {
      throw new ISE("tableMemory must be ByteBuffer-backed");
    }

    if (!buffer.order().equals(ByteOrder.BIG_ENDIAN)) {
      throw new ISE("tableMemory's ByteBuffer must be in big-endian order");
    }

    if (buffer.capacity() != memory.getCapacity() || buffer.remaining() != buffer.capacity()) {
      throw new ISE("tableMemory's ByteBuffer must be coterminous");
    }
  }

  public static int memoryNeeded(final int numBuckets, final int bucketSize)
  {
    return numBuckets * bucketSize + (int) Math.ceil((float) numBuckets / Byte.SIZE);
  }

  private boolean isBucketUsed(int bucket)
  {
    return getBucketUsed(bucket) != 0; // can be negative
  }

  private int getBucketUsed(int bucket)
  {
    final int container = bucket / Byte.SIZE;
    final int offset = bucket % Byte.SIZE;
    return getFlagFromContainer(flagMemory.getByte(container), offset);
  }

  private byte getFlagFromContainer(byte container, int offset)
  {
    return (byte) (container & (1 << offset));
  }

  private void setUsedFlag(int bucket)
  {
    final int container = bucket / Byte.SIZE;
    final int offset = bucket % Byte.SIZE;
    flagMemory.putByte(container, (byte) (flagMemory.getByte(container) | (byte) (1 << offset)));
  }

  public int findBucket(final int keyHash, final Memory keySpace, final int keySpacePosition)
  {
    int bucket = keyHash & bucketMask;

    while (true) {
      if (!isBucketUsed(bucket)) {
        // Found unused bucket before finding our key.
        return -bucket - 1;
      }

      final int bucketOffset = bucket * bucketSize;
      final boolean keyFound = HashTableUtils.memoryEquals(
          bucketMemory,
          bucketOffset,
          keySpace,
          keySpacePosition,
          keySize
      );

      if (keyFound) {
        return bucket;
      }

      bucket = (bucket + 1) & bucketMask;
    }
  }

  public boolean canInsertNewBucket()
  {
    return size < maxSize;
  }

  public void initBucket(final int bucket, final Memory keySpace, final int keySpacePosition)
  {
    // Method preconditions.
    assert canInsertNewBucket() && !isBucketUsed(bucket);

    final int bucketOffset = bucketMemoryPosition(bucket);

    // Mark the bucket used and write in the key.
    setUsedFlag(bucket);
    keySpace.copyTo(keySpacePosition, bucketMemory, bucketOffset, keySize);
    size++;
  }

  public int size()
  {
    return size;
  }

  public int numBuckets()
  {
    return numBuckets;
  }

  public int keySize()
  {
    return keySize;
  }

  /**
   * Returns the size of values, in bytes.
   */
  public int valueSize()
  {
    return valueSize;
  }

  public int bucketArenaOffset()
  {
    return (int) flagMemory.getCapacity();
  }

  /**
   * Returns the offset within each bucket where the key starts.
   */
  public int bucketKeyOffset()
  {
    return 0;
  }

  /**
   * Returns the offset within each bucket where the value starts.
   */
  public int bucketValueOffset()
  {
    return keySize;
  }

  /**
   * Returns the size in bytes of each bucket.
   */
  public int bucketSize()
  {
    return bucketSize;
  }

  /**
   * Returns the position within {@link #memory()} where a particular bucket starts.
   */
  public int bucketMemoryPosition(final int bucket)
  {
    return bucket * bucketSize;
  }

  /**
   * Returns the memory backing this table.
   */
  public WritableMemory memory()
  {
    return bucketMemory;
  }

  public IntIterator bucketIterator()
  {
    return new IntIterator()
    {
      private int curr = 0;
//      private int currBucket = -1;
      private byte currContainer;
      private int containerOffset = -1;
      private int bitOffset = Byte.SIZE - 1;

      @Override
      public boolean hasNext()
      {
        return curr < size;
      }

      @Override
      public int nextInt()
      {
        if (curr >= size) {
          throw new NoSuchElementException();
        }

        bitOffset++;
        seekNextContainer();

        while (getFlagFromContainer(currContainer, bitOffset) == 0) {
          bitOffset++;
          seekNextContainer(); // TODO: can filter early instead of doing this
//          assert bitOffset < Byte.SIZE;
        }

        curr++;
        return containerOffset * Byte.SIZE + bitOffset;
      }

      private void seekNextContainer()
      {
        if (bitOffset == Byte.SIZE) {
          containerOffset++;
          bitOffset = 0;

          while ((currContainer = flagMemory.getByte(containerOffset)) == 0) {
            containerOffset++;
          }
        }
      }
    };
  }
}
