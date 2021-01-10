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
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.MemoryVectorAggregators;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.MemoryVectorEntry;
import org.apache.druid.query.groupby.epinephelinae.collection.HashTableUtils;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable2;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedSizeHashVectorGrouper implements VectorGrouper
{
  private static final int MIN_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  private boolean initialized = false;
  private int maxVectorSize;
  private int maxNumBuckets;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final int numTables;
  private final MemoryVectorAggregators aggregators;
  private final int keySize;
  private final int bufferGrouperMaxSize;
  private final int configuredInitialNumBuckets;
  private final int bucketSize;
  private final float maxLoadFactor;

  private ByteBuffer totalBuffer;

  private HashTableStuff[] hashTables;

  private static class HashTableStuff
  {
    private final ByteBuffer bufferOrigin;
    private ByteBuffer buffer;
    private int start;
    private MemoryOpenHashTable2 hashTable;

    private HashTableStuff(ByteBuffer bufferOrigin, ByteBuffer buffer, int start, MemoryOpenHashTable2 hashTable)
    {
      this.bufferOrigin = bufferOrigin;
      this.buffer = buffer;
      this.start = start;
      this.hashTable = hashTable;
    }
  }

  @Nullable
  private int[] vAllKeyHashCodes = null;
  // Scratch objects used by aggregateVector(). Set by initVectorized().
  @Nullable
  private int[] vKeyHashCodes = null;
  @Nullable
  private int[] vAggregationPositions = null;
  @Nullable
  private int[] vAggregationRows = null;
  @Nullable
  private int[] vHashedRows = null;

  public FixedSizeHashVectorGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final int numTables,
      final int keySize,
      final MemoryVectorAggregators aggregators,
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
    this.bucketSize = MemoryOpenHashTable2.bucketSize(keySize, aggregators.spaceNeeded());
    this.hashTables = new HashTableStuff[numTables];

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }
  }

  @Override
  public void initVectorized(int maxVectorSize)
  {
    if (!initialized) {
      totalBuffer = bufferSupplier.get();
      final int sliceSize = totalBuffer.capacity() / numTables;
      this.maxNumBuckets = Math.max(
          HashVectorGrouper.computeRoundedInitialNumBuckets(sliceSize, bucketSize, configuredInitialNumBuckets),
          HashVectorGrouper.computeMaxNumBucketsAfterGrowth(sliceSize, bucketSize)
      );

      reset();

      this.vAllKeyHashCodes = new int[maxVectorSize];
      this.vKeyHashCodes = new int[maxVectorSize];
      this.vAggregationPositions = new int[maxVectorSize];
      this.vAggregationRows = new int[maxVectorSize];
      this.vHashedRows = new int[maxVectorSize];
      this.maxVectorSize = maxVectorSize;

      initialized = true;
    }
  }

  private int hashTablePointer(int keyHashCode)
  {
    return Math.abs(keyHashCode) % hashTables.length;
  }

  @Override
  public AggregateResult aggregateVector(Memory keySpace, int startRow, int endRow)
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
      vAllKeyHashCodes[rowNum] = Groupers.smear(HashTableUtils.hashMemory(keySpace, keySpacePosition, keySize));
    }

    for (int hashTablePointer = 0; hashTablePointer < hashTables.length; hashTablePointer++) {
      int i = 0;
      for (int rowNum = 0; rowNum < numRows; rowNum++) {
        if (hashTablePointer(vAllKeyHashCodes[rowNum]) == hashTablePointer) {
          vKeyHashCodes[i] = vAllKeyHashCodes[rowNum];
          vHashedRows[i++] = rowNum;
        }
      }
      final int numRowsToProcess = i;
//      System.err.println("hashTablePointer: " + hashTablePointer + " vKeyHashCodes: " + Arrays.toString(vKeyHashCodes) + " numRowsToProcess: " + numRowsToProcess + " startRow: " + startRow + ", numRows: " + numRows);
      final HashTableStuff hashTableStuff = hashTables[hashTablePointer];

      final AggregateResult partialResult = aggregateOneTable(keySpace, startRow, numRowsToProcess, hashTableStuff);
      if (!partialResult.isOk()) {
        return partialResult;
      }
    }

    return AggregateResult.ok();
  }

  private AggregateResult aggregateOneTable(final Memory keySpace, final int startRowOffset, final int numRows, final HashTableStuff hashTableStuff)
  {
    int aggregationStartRow = 0; // pointer to vHashedRows
    int aggregationNumRows = 0; // numRows to aggregate in vHashedRows

    final int aggregatorStartOffset = hashTableStuff.hashTable.bucketValueOffset();

    for (int rowNum = 0; rowNum < numRows; rowNum++) {
      final int keySpacePosition = vHashedRows[rowNum] * keySize;
      // Find, and if the table is full, expand and find again.
      int bucket = hashTableStuff.hashTable.findBucket(vKeyHashCodes[rowNum], keySpace, keySpacePosition);
//      System.err.println("Aggregating hash " + vKeyHashCodes[rowNum]);

      if (bucket < 0) {
        // Bucket not yet initialized.
        if (hashTableStuff.hashTable.canInsertNewBucket()) {
          // There's space, initialize it and move on.
          bucket = -(bucket + 1);
          initBucket(hashTableStuff.hashTable, bucket, keySpace, keySpacePosition);
        } else {
          // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message
          // will be correct.
          return Groupers.hashTableFull(rowNum);
        }
      }

      // Schedule the current row for aggregation.
      vAggregationPositions[aggregationNumRows] = bucket * bucketSize + aggregatorStartOffset;
//      System.err.println("table: " + hashTableStuff.hashTable + ", bucket " + bucket + ", pos: " + vAggregationPositions[aggregationNumRows]);
      aggregationNumRows++;
    }

    // Aggregate any remaining rows.
    if (aggregationNumRows > 0) {
//      System.err.println("aggregate remaining");
      doAggregateVector(hashTableStuff, startRowOffset, aggregationStartRow, aggregationNumRows);
    }

    return AggregateResult.ok();
  }

  private void initBucket(MemoryOpenHashTable2 hashTable, final int bucket, final Memory keySpace, final int keySpacePosition)
  {
    assert bucket >= 0 && bucket < maxNumBuckets && hashTable != null && hashTable.canInsertNewBucket();
//    System.err.println("init bucket: " + hashTable + ", bucket " + bucket + ", table buffer: " + hashTable.memory().getByteBuffer() + ", init pos: " + (bucket * bucketSize + hashTable.bucketValueOffset()));
    hashTable.initBucket(bucket, keySpace, keySpacePosition);
    aggregators.init(hashTable.memory(), bucket * bucketSize + hashTable.bucketValueOffset());
  }

  private void doAggregateVector(HashTableStuff hashTableStuff, int startRowOffset, final int startRow, final int numRows)
  {
    for (int i = 0; i < numRows; i++) {
      vAggregationRows[i] = vHashedRows[i + startRow] + startRowOffset;
    }
    aggregators.aggregateVector(
        hashTableStuff.hashTable.memory(),
        numRows,
        vAggregationPositions,
//        Groupers.writeAggregationRows(vAggregationRows, startRow, startRow + numRows)
        vAggregationRows
    );
  }

  @Override
  public void reset()
  {
    final int sliceSize = totalBuffer.capacity() / numTables;
    final ByteBuffer duplicateTotalBuffer = totalBuffer.duplicate();

    for (int i = 0; i < numTables; i++) {
      // Compute initial hash table size (numBuckets).
      final int numBuckets = maxNumBuckets;
      final int tableStart = 0;

      final ByteBuffer bufferOrigin = Groupers.getSlice(duplicateTotalBuffer, sliceSize, i);
      final ByteBuffer bufferSlice = bufferOrigin.duplicate();

      bufferSlice.position(0);
      bufferSlice.limit(MemoryOpenHashTable2.memoryNeeded(numBuckets, bucketSize));

      final MemoryOpenHashTable2 hashTable = new MemoryOpenHashTable2(
          WritableMemory.wrap(bufferSlice.slice(), ByteOrder.nativeOrder()),
          numBuckets,
          Math.max(1, Math.min(bufferGrouperMaxSize, (int) (numBuckets * maxLoadFactor))),
          keySize,
          aggregators.spaceNeeded()
      );
      hashTables[i] = new HashTableStuff(bufferOrigin, bufferSlice, tableStart, hashTable);
    }
  }

  @Override
  public void close()
  {
    aggregators.close();
  }

  @Override
  public CloseableIterator<Entry<Memory>> iterator(@Nullable MemoryComparator comparator)
  {
    throw new UnsupportedOperationException();
  }

  public List<CloseableIterator<MemoryVectorEntry>> iterators(int segmentId)
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      return IntStream
          .range(0, numTables)
          .mapToObj(i -> CloseableIterators.withEmptyBaggage(Collections.<MemoryVectorEntry>emptyIterator()))
          .collect(Collectors.toList());
    }

    final WritableMemory keyVector = WritableMemory.wrap(new byte[keySize * maxVectorSize]);
    final List<CloseableIterator<MemoryVectorEntry>> iterators = new ArrayList<>(numTables);
    for (int i = 0; i < numTables; i++) {
      final MemoryOpenHashTable2 hashTable = hashTables[i].hashTable;
//      System.err.println(Thread.currentThread() + ", hash table size: " + hashTables[i].hashTable.size());
      final IntIterator baseIterator = hashTable.bucketIterator();
      iterators.add(
          CloseableIterators.withEmptyBaggage(
              new Iterator<MemoryVectorEntry>()
              {
                @Override
                public boolean hasNext()
                {
                  return baseIterator.hasNext();
                }

                @Override
                public MemoryVectorEntry next()
                {
                  // TODO: which is better between making a copy and indirect read?
                  final Object[][] valuess = new Object[maxVectorSize][];
                  int curVectorSize = 0;
                  for (; curVectorSize < maxVectorSize && baseIterator.hasNext(); curVectorSize++) {
                    final int bucket = baseIterator.nextInt();
                    final int bucketPosition = hashTable.bucketMemoryPosition(bucket);

                    hashTable.memory().copyTo(
                        bucketPosition + hashTable.bucketKeyOffset(),
                        keyVector,
                        curVectorSize * keySize,
                        keySize
                    );

                    final Object[] values = new Object[aggregators.size()];
                    final int aggregatorsOffset = bucketPosition + hashTable.bucketValueOffset();
                    for (int i = 0; i < aggregators.size(); i++) {
                      values[i] = aggregators.get(hashTable.memory(), aggregatorsOffset, i);
                    }
                    valuess[curVectorSize] = values;
                  }

//                  System.err.println(Thread.currentThread() + ", row: " + Groupers.deserializeToRow(segmentId, keyVector, valuess));
                  return new MemoryVectorEntry(keyVector, valuess, maxVectorSize, curVectorSize, segmentId);
                }
              }
          )
      );
    }
    return iterators;
  }
}
