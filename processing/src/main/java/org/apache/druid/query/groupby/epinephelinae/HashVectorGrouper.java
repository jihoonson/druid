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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.collection.HashTableUtils;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * An implementation of {@link VectorGrouper} backed by a growable {@link MemoryOpenHashTable}. Growability is
 * implemented in this class because {@link MemoryOpenHashTable} is not innately growable.
 */
public class HashVectorGrouper implements VectorGrouper
{
  private static final int MIN_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  private boolean initialized = false;
  private int maxNumBuckets;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final int numTables;
  private final AggregatorAdapters aggregators;
  private final int keySize;
  private final int bufferGrouperMaxSize;
  private final int configuredInitialNumBuckets;
  private final int bucketSize;
  private final float maxLoadFactor;

//  private ByteBuffer buffer;
//  private int tableStart = 0;

  private ByteBuffer totalBuffer;

  private HashTableStuff[] hashTables;

  private static class HashTableStuff
  {
    private final ByteBuffer bufferOrigin;
    private ByteBuffer buffer;
    private int start;
    private MemoryOpenHashTable hashTable;

    private HashTableStuff(ByteBuffer bufferOrigin, ByteBuffer buffer, int start, MemoryOpenHashTable hashTable)
    {
      this.bufferOrigin = bufferOrigin;
      this.buffer = buffer;
      this.start = start;
      this.hashTable = hashTable;
    }
  }

  // Scratch objects used by aggregateVector(). Set by initVectorized().
  @Nullable
  private int[] vKeyHashCodes = null;
  @Nullable
  private int[] vAggregationPositions = null;
  @Nullable
  private int[] vAggregationRows = null;
  @Nullable
  private int[] vAggregatorStartOffset = null;
  @Nullable
  private HashTableStuff[] vHashTables = null;
  @Nullable
  private ByteBuffer[] vHashTableBuffers = null;

  public HashVectorGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final int numTables,
      final int keySize,
      final AggregatorAdapters aggregators,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int configuredInitialNumBuckets
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.numTables = numTables;
    this.keySize = keySize;
    this.aggregators = aggregators;
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.configuredInitialNumBuckets = configuredInitialNumBuckets >= MIN_BUCKETS
                                       ? configuredInitialNumBuckets
                                       : DEFAULT_INITIAL_BUCKETS;
    this.bucketSize = MemoryOpenHashTable.bucketSize(keySize, aggregators.spaceNeeded());
    this.hashTables = new HashTableStuff[numTables];

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }
  }

  @Override
  public void initVectorized(final int maxVectorSize)
  {
    if (!initialized) {
      totalBuffer = bufferSupplier.get();
      final int sliceSize = totalBuffer.capacity() / numTables;
      this.maxNumBuckets = Math.max(
          computeRoundedInitialNumBuckets(sliceSize, bucketSize, configuredInitialNumBuckets),
          computeMaxNumBucketsAfterGrowth(sliceSize, bucketSize)
      );

      reset();

      this.vKeyHashCodes = new int[maxVectorSize];
      this.vAggregationPositions = new int[maxVectorSize];
      this.vAggregationRows = new int[maxVectorSize];
      this.vAggregatorStartOffset = new int[maxVectorSize];
      this.vHashTables = new HashTableStuff[maxVectorSize];
      this.vHashTableBuffers = new ByteBuffer[maxVectorSize];

      initialized = true;
    }
  }

  private HashTableStuff getHashTable(int keyHashCode)
  {
    return hashTables[Math.abs(keyHashCode) % hashTables.length];
  }

  @Override
  public AggregateResult aggregateVector(final Memory keySpace, final int startRow, final int endRow)
  {
    final int numRows = endRow - startRow;

    // Hoisted bounds check on keySpace.
    if (keySpace.getCapacity() < (long) numRows * keySize) {
      throw new IAE("Not enough keySpace capacity for the provided start/end rows");
    }

    // We use integer indexes into the keySpace.
    if (keySpace.getCapacity() > Integer.MAX_VALUE) {
      throw new ISE("keySpace too large to handle");
    }

    // Initialize vKeyHashCodes: one int per key.
    // Does *not* use hashFunction(). This is okay because the API of VectorGrouper does not expose any way of messing
    // about with hash codes.
    for (int rowNum = 0, keySpacePosition = 0; rowNum < numRows; rowNum++, keySpacePosition += keySize) {
      vKeyHashCodes[rowNum] = Groupers.smear(HashTableUtils.hashMemory(keySpace, keySpacePosition, keySize));
    }
    resetHashTableStuffs(numRows);

    System.err.println(Thread.currentThread() + ", " + Arrays.toString(vKeyHashCodes));

    int aggregationStartRow = startRow;
    int aggregationNumRows = 0;

    for (int rowNum = 0, keySpacePosition = 0; rowNum < numRows; rowNum++, keySpacePosition += keySize) {
      // Find, and if the table is full, expand and find again.
      int bucket = vHashTables[rowNum].hashTable.findBucket(vKeyHashCodes[rowNum], keySpace, keySpacePosition);

      if (bucket < 0) {
        // Bucket not yet initialized.
        if (vHashTables[rowNum].hashTable.canInsertNewBucket()) {
          // There's space, initialize it and move on.
          bucket = -(bucket + 1);
          initBucket(vHashTables[rowNum].hashTable, bucket, keySpace, keySpacePosition);
        } else {
          // Out of space. Finish up unfinished aggregations, then try to grow.
          if (aggregationNumRows > 0) {
            doAggregateVector(aggregationStartRow, aggregationNumRows);
            aggregationStartRow = aggregationStartRow + aggregationNumRows;
            aggregationNumRows = 0;
          }

          if (grow(vHashTables[rowNum]) && vHashTables[rowNum].hashTable.canInsertNewBucket()) {
            resetHashTableStuffs(numRows);
            bucket = vHashTables[rowNum].hashTable.findBucket(vKeyHashCodes[rowNum], keySpace, keySpacePosition);
            bucket = -(bucket + 1);
            initBucket(vHashTables[rowNum].hashTable, bucket, keySpace, keySpacePosition);
          } else {
            // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message
            // will be correct.
            return Groupers.hashTableFull(rowNum);
          }
        }
      }

      // Schedule the current row for aggregation.
      vAggregationPositions[aggregationNumRows] = bucket * bucketSize + vAggregatorStartOffset[rowNum];
      System.err.println("table: " + vHashTables[rowNum].hashTable + ", bucket " + bucket + ", pos: " + vAggregationPositions[aggregationNumRows]);
      aggregationNumRows++;
    }

    // Aggregate any remaining rows.
    if (aggregationNumRows > 0) {
      System.err.println("aggregate remaining");
      doAggregateVector(aggregationStartRow, aggregationNumRows);
    }

    return AggregateResult.ok();
  }

  private void resetHashTableStuffs(int numRows)
  {
    for (int rowNum = 0, keySpacePosition = 0; rowNum < numRows; rowNum++, keySpacePosition += keySize) {
      vHashTables[rowNum] = getHashTable(vKeyHashCodes[rowNum]);
      vHashTableBuffers[rowNum] = vHashTables[rowNum].hashTable.memory().getByteBuffer();
      vAggregatorStartOffset[rowNum] = vHashTables[rowNum].hashTable.bucketValueOffset();
    }
  }

  @Override
  public void reset()
  {
    final int sliceSize = totalBuffer.capacity() / numTables;
    final ByteBuffer duplicateTotalBuffer = totalBuffer.duplicate();

    for (int i = 0; i < numTables; i++) {
      // Compute initial hash table size (numBuckets).
      final int numBuckets = computeRoundedInitialNumBuckets(sliceSize, bucketSize, configuredInitialNumBuckets);
      assert numBuckets <= maxNumBuckets;

      final int tableStart;
      if (numBuckets == maxNumBuckets) {
        // Maximum-sized tables start at zero.
        tableStart = 0;
      } else {
        // The first table, if not maximum-sized, starts at the latest possible position (where the penultimate
        // table ends at the end of the buffer).
        tableStart = sliceSize - bucketSize * (maxNumBuckets - numBuckets);
      }

      final ByteBuffer bufferOrigin = Groupers.getSlice(duplicateTotalBuffer, sliceSize, i);
      final ByteBuffer bufferSlice = bufferOrigin.duplicate();

      bufferSlice.position(0);
      bufferSlice.limit(MemoryOpenHashTable.memoryNeeded(numBuckets, bucketSize));

      final MemoryOpenHashTable hashTable = new MemoryOpenHashTable(
          WritableMemory.wrap(bufferSlice.slice(), ByteOrder.nativeOrder()),
          numBuckets,
          Math.max(1, Math.min(bufferGrouperMaxSize, (int) (numBuckets * maxLoadFactor))),
          keySize,
          aggregators.spaceNeeded()
      );
      hashTables[i] = new HashTableStuff(bufferOrigin, bufferSlice, tableStart, hashTable);
    }
  }

  public List<CloseableIterator<Entry<Memory>>> iterators()
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      final List<CloseableIterator<Entry<Memory>>> emptyIterators = IntStream
          .range(0, numTables)
          .mapToObj(i -> CloseableIterators.withEmptyBaggage(Collections.<Entry<Memory>>emptyIterator()))
          .collect(Collectors.toList());
      return emptyIterators;
    }

    final List<CloseableIterator<Entry<Memory>>> iterators = new ArrayList<>(numTables);
    for (int i = 0; i < numTables; i++) {
      final MemoryOpenHashTable hashTable = hashTables[i].hashTable;
      System.err.println(Thread.currentThread() + ", hash table size: " + hashTables[i].hashTable.size());
      final IntIterator baseIterator = hashTable.bucketIterator();
      iterators.add(
          CloseableIterators.withEmptyBaggage(
              new Iterator<Entry<Memory>>()
              {
                @Override
                public boolean hasNext()
                {
                  return baseIterator.hasNext();
                }

                @Override
                public Entry<Memory> next()
                {
                  final int bucket = baseIterator.nextInt();
                  final int bucketPosition = hashTable.bucketMemoryPosition(bucket);

                  final Memory keyMemory = hashTable.memory().region(
                      bucketPosition + hashTable.bucketKeyOffset(),
                      hashTable.keySize()
                  );

                  final Object[] values = new Object[aggregators.size()];
                  final int aggregatorsOffset = bucketPosition + hashTable.bucketValueOffset();
                  for (int i = 0; i < aggregators.size(); i++) {
                    values[i] = aggregators.get(hashTable.memory().getByteBuffer(), aggregatorsOffset, i);
                  }

                  return new Grouper.Entry<>(keyMemory, values);
                }
              }
          )
      );
    }
    return iterators;
  }

  @Override
  public CloseableIterator<Grouper.Entry<Memory>> iterator(@Nullable MemoryComparator comparator)
  {
    if (comparator != null) {
      return sortedIterator(comparator);
    }

    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    final Iterator<HashTableStuff> hashTableIterator = Iterators.forArray(hashTables);

    return new CloseableIterator<Grouper.Entry<Memory>>()
    {
      HashTableStuff current;
      @Nullable
      IntIterator currBucketIterator = findNextIteratorIfNecessary();

      @Nullable
      private IntIterator findNextIteratorIfNecessary()
      {
        while ((currBucketIterator == null || !currBucketIterator.hasNext()) && hashTableIterator.hasNext()) {
          if (currBucketIterator != null) {
            currBucketIterator = null;
          }
          current = hashTableIterator.next();
          currBucketIterator = current.hashTable.bucketIterator();
          if (currBucketIterator.hasNext()) {
            return currBucketIterator;
          }
        }
        return null;
      }

      @Override
      public boolean hasNext()
      {
        return currBucketIterator != null && currBucketIterator.hasNext();
      }

      @Override
      public Grouper.Entry<Memory> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          final int bucket = currBucketIterator.nextInt();
          final int bucketPosition = current.hashTable.bucketMemoryPosition(bucket);

          final Memory keyMemory = current.hashTable.memory().region(
              bucketPosition + current.hashTable.bucketKeyOffset(),
              current.hashTable.keySize()
          );

          final Object[] values = new Object[aggregators.size()];
          final int aggregatorsOffset = bucketPosition + current.hashTable.bucketValueOffset();
          for (int i = 0; i < aggregators.size(); i++) {
            values[i] = aggregators.get(current.hashTable.memory().getByteBuffer(), aggregatorsOffset, i);
          }

          return new Grouper.Entry<>(keyMemory, values);
        }
        finally {
          findNextIteratorIfNecessary();
        }
      }

      @Override
      public void close()
      {
        // Do nothing.
      }
    };
  }

  private CloseableIterator<Grouper.Entry<Memory>> sortedIterator(MemoryComparator comparator)
  {
    throw new UnsupportedOperationException();
//    assert initialized;
//
//    final IntList offsetList = new IntArrayList(hashTables.size());
//    hashTables.bucketIterator().forEachRemaining((IntConsumer) offsetList::add);
//
//    // Sort offsets in-place.
//    Collections.sort(
//        offsetList,
//        (lhs, rhs) -> {
//          final int lhsPos = hashTables.bucketMemoryPosition(lhs);
//          final int rhsPos = hashTables.bucketMemoryPosition(rhs);
//
//          return comparator.compare(
//              hashTables.memory(),
//              hashTables.memory(),
//              lhsPos + hashTables.bucketKeyOffset(),
//              rhsPos + hashTables.bucketKeyOffset()
//          );
//        }
//    );
//
//    return new CloseableIterator<Entry<Memory>>()
//    {
//      final IntIterator baseIterator = offsetList.iterator();
//
//      @Override
//      public boolean hasNext()
//      {
//        return baseIterator.hasNext();
//      }
//
//      @Override
//      public Entry<Memory> next()
//      {
//        if (!hasNext()) {
//          throw new NoSuchElementException();
//        }
//
//        final int bucket = baseIterator.nextInt();
//        final int bucketPosition = hashTables.bucketMemoryPosition(bucket);
//
//        final Memory keyMemory = hashTables.memory().region(
//            bucketPosition + hashTables.bucketKeyOffset(),
//            hashTables.keySize()
//        );
//
//        final Object[] values = new Object[aggregators.size()];
//        final int aggregatorsOffset = bucketPosition + hashTables.bucketValueOffset();
//        for (int i = 0; i < aggregators.size(); i++) {
//          values[i] = aggregators.get(hashTables.memory().getByteBuffer(), aggregatorsOffset, i);
//        }
//
//        return new Grouper.Entry<>(keyMemory, values);
//      }
//
//      @Override
//      public void remove()
//      {
//        throw new UnsupportedOperationException();
//      }
//
//      @Override
//      public void close()
//      {
//        // do nothing
//      }
//    };
  }

  @Override
  public void close()
  {
    aggregators.close();
  }


  /**
   * Initializes the given bucket with the given key and fresh, empty aggregation state. Must only be called if
   * {@code hashTable.canInsertNewBucket()} returns true and if this bucket is currently unused.
   */
  private void initBucket(MemoryOpenHashTable hashTable, final int bucket, final Memory keySpace, final int keySpacePosition)
  {
    assert bucket >= 0 && bucket < maxNumBuckets && hashTable != null && hashTable.canInsertNewBucket();
    System.err.println("init bucket: " + hashTable + ", bucket " + bucket + ", table buffer: " + hashTable.memory().getByteBuffer() + ", init pos: " + (bucket * bucketSize + hashTable.bucketValueOffset()));
    hashTable.initBucket(bucket, keySpace, keySpacePosition);
    aggregators.init(hashTable.memory().getByteBuffer(), bucket * bucketSize + hashTable.bucketValueOffset());
  }

  /**
   * Aggregate the current vector from "startRow" (inclusive) to "endRow" (exclusive) into aggregation positions
   * given by {@link #vAggregationPositions}.
   */
  private void doAggregateVector(final int startRow, final int numRows)
  {
    System.err.println(Thread.currentThread() + ", startRow: " + startRow + " numRows: " + numRows);
    aggregators.aggregateVector(
        vHashTableBuffers,
        numRows,
        vAggregationPositions,
        Groupers.writeAggregationRows(vAggregationRows, startRow, startRow + numRows)
    );
  }

  /**
   * Attempts to grow the table and returns whether or not it was possible. Each growth doubles the number of buckets
   * in the table.
   */
  private boolean grow(HashTableStuff hashTableStuff)
  {
    if (hashTableStuff.hashTable.numBuckets() >= maxNumBuckets) {
      return false;
    }

    final int newNumBuckets = nextTableNumBuckets(hashTableStuff.hashTable);
    final int newTableStart = nextTableStart(hashTableStuff);

    final ByteBuffer newTableBuffer = hashTableStuff.bufferOrigin.duplicate();
    newTableBuffer.position(newTableStart);
    newTableBuffer.limit(newTableStart + MemoryOpenHashTable.memoryNeeded(newNumBuckets, bucketSize));

    final MemoryOpenHashTable newHashTable = new MemoryOpenHashTable(
        WritableMemory.wrap(newTableBuffer.slice(), ByteOrder.nativeOrder()),
        newNumBuckets,
        maxSizeForNumBuckets(newNumBuckets, maxLoadFactor, bufferGrouperMaxSize),
        keySize,
        aggregators.spaceNeeded()
    );

    hashTableStuff.hashTable.copyTo(newHashTable, new HashVectorGrouperBucketCopyHandler(aggregators, hashTableStuff.hashTable.bucketValueOffset()));
    System.err.println("growing " + hashTableStuff.hashTable + " -> " + newHashTable);
    System.err.println("growing " + hashTableStuff.hashTable.size() + " -> " + newHashTable.size());
    hashTableStuff.hashTable = newHashTable;
    hashTableStuff.start = newTableStart;
    hashTableStuff.buffer = newTableBuffer;
    return true;
  }

  /**
   * Returns the table size after the next growth. Each growth doubles the number of buckets, so this will be
   * double the current number of buckets.
   *
   * @throws IllegalStateException if not initialized or if growing is not possible
   */
  private int nextTableNumBuckets(MemoryOpenHashTable hashTable)
  {
    if (!initialized) {
      throw new ISE("Must be initialized");
    }

    if (hashTable.numBuckets() >= maxNumBuckets) {
      throw new ISE("No room left to grow");
    }

    return hashTable.numBuckets() * 2;
  }

  /**
   * Returns the start of the table within {@link #buffer} after the next growth. Each growth starts from the end of
   * the previous table.
   *
   * @throws IllegalStateException if not initialized or if growing is not possible
   */
  private int nextTableStart(HashTableStuff hashTableStuff)
  {
    if (!initialized) {
      throw new ISE("Must be initialized");
    }

    final int nextNumBuckets = nextTableNumBuckets(hashTableStuff.hashTable);
    final int currentEnd = hashTableStuff.start + MemoryOpenHashTable.memoryNeeded(hashTableStuff.hashTable.numBuckets(), bucketSize);

    final int nextTableStart;

    if (nextNumBuckets == maxNumBuckets) {
      assert currentEnd == hashTableStuff.buffer.capacity();
      nextTableStart = 0;
    } else {
      nextTableStart = currentEnd;
    }

    // Sanity check on buffer capacity. If this triggers then it is a bug in this class.
    final long nextEnd = ((long) nextTableStart) + MemoryOpenHashTable.memoryNeeded(nextNumBuckets, bucketSize);

    if (nextEnd > hashTableStuff.buffer.capacity()) {
      throw new ISE("New table overruns buffer capacity");
    }

    if (nextTableStart < currentEnd && nextEnd > hashTableStuff.start) {
      throw new ISE("New table overruns old table");
    }

    return nextTableStart;
  }

  /**
   * Compute the maximum number of elements (size) for a given number of buckets. When the table hits this size,
   * we must either grow it or return a table-full error.
   */
  private static int maxSizeForNumBuckets(final int numBuckets, final double maxLoadFactor, final int configuredMaxSize)
  {
    return Math.max(1, Math.min(configuredMaxSize, (int) (numBuckets * maxLoadFactor)));
  }

  /**
   * Compute the initial table bucket count given a particular buffer capacity, bucket size, and user-configured
   * initial bucket count.
   *
   * @param capacity                    buffer capacity, in bytes
   * @param bucketSize                  bucket size, in bytes
   * @param configuredInitialNumBuckets user-configured initial bucket count
   */
  private static int computeRoundedInitialNumBuckets(
      final int capacity,
      final int bucketSize,
      final int configuredInitialNumBuckets
  )
  {
    final int initialNumBucketsRoundedUp = (int) Math.min(
        1 << 30,
        HashCommon.nextPowerOfTwo((long) configuredInitialNumBuckets)
    );

    if (initialNumBucketsRoundedUp < computeMaxNumBucketsAfterGrowth(capacity, bucketSize)) {
      return initialNumBucketsRoundedUp;
    } else {
      // Special case: initialNumBucketsRoundedUp is equal to or higher than max capacity of a growable table; start out
      // at max size the buffer will hold. Note that this allows the table to be larger than it could ever be as a
      // result of growing, proving that biting off as much as you can chew is not always a bad strategy. (Why don't
      // we always do this? Because clearing a big table is expensive.)
      return HashTableUtils.previousPowerOfTwo(Math.min(capacity / bucketSize, 1 << 30));
    }
  }

  /**
   * Compute the largest possible table bucket count given a particular buffer capacity, bucket size, and initial
   * bucket count. Assumes that tables are grown by allocating new tables that are twice as large and then copying
   * into them.
   *
   * @param capacity   buffer capacity, in bytes
   * @param bucketSize bucket size, in bytes
   */
  private static int computeMaxNumBucketsAfterGrowth(final int capacity, final int bucketSize)
  {
    // Tables start at some size (see computeRoundedInitialNumBuckets) and grow by doubling. The penultimate table ends
    // at the end of the buffer, and then the final table starts at the beginning of the buffer. This means the largest
    // possible table size 2^x is the one where x is maximized subject to:
    //
    //   2^(x-1) < capacity / bucketSize / 3
    //
    // Or:
    //
    //   2^x < capacity / bucketSize / 3 * 2
    //
    // All other smaller tables fit within the 2/3rds of the buffer preceding the penultimate table, and then the
    // memory they used can be reclaimed for the final table.
    return HashTableUtils.previousPowerOfTwo(Math.min(capacity / bucketSize / 3 * 2, 1 << 30));
  }

  private static class HashVectorGrouperBucketCopyHandler implements MemoryOpenHashTable.BucketCopyHandler
  {
    private final AggregatorAdapters aggregators;
    private final int baseAggregatorOffset;

    public HashVectorGrouperBucketCopyHandler(final AggregatorAdapters aggregators, final int bucketAggregatorOffset)
    {
      this.aggregators = aggregators;
      this.baseAggregatorOffset = bucketAggregatorOffset;
    }

    @Override
    public void bucketCopied(
        final int oldBucket,
        final int newBucket,
        final MemoryOpenHashTable oldTable,
        final MemoryOpenHashTable newTable
    )
    {
      // Relocate aggregators (see https://github.com/apache/druid/pull/4071).
      aggregators.relocate(
          oldTable.bucketMemoryPosition(oldBucket) + baseAggregatorOffset,
          newTable.bucketMemoryPosition(newBucket) + baseAggregatorOffset,
          oldTable.memory().getByteBuffer(),
          newTable.memory().getByteBuffer()
      );
    }
  }
}
