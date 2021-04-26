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

import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.aggregation.MemoryVectorAggregators;
import org.apache.druid.query.groupby.epinephelinae.Grouper.MemoryVectorEntry;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryOpenHashTable2;

public class HashTableIterator implements SizedIterator<MemoryVectorEntry>
{
  private final int segmentId;
  private final MemoryOpenHashTable2 hashTable;
  private final MemoryVectorAggregators aggregators;
  private final int maxVectorSize;
  private final IntIterator baseIterator;
  private final WritableMemory keyVector;

  private int nextVectorId = 0;

  public HashTableIterator(
      int segmentId,
      MemoryOpenHashTable2 hashTable,
      MemoryVectorAggregators aggregators,
      int maxVectorSize,
      int keySize
  )
  {
    this.segmentId = segmentId;
    this.hashTable = hashTable;
    this.aggregators = aggregators;
    this.maxVectorSize = maxVectorSize;
    this.baseIterator = hashTable.bucketIterator();
    // each table should create a new keyVector so that different threads can work on different tables
    this.keyVector = WritableMemory.wrap(new byte[keySize * maxVectorSize]);
  }

  @Override
  public boolean hasNext()
  {
    return baseIterator.hasNext();
  }

  @Override
  public MemoryVectorEntry next()
  {
    // TODO: hmm... can it be prettier?
    final double[][] doubleValues = new double[aggregators.size()][];
    final float[][] floatValues = new float[aggregators.size()][];
    final long[][] longValues = new long[aggregators.size()][];
    final Object[] valuess = new Object[aggregators.size()];
    for (int i = 0; i < aggregators.size(); i++) {
      switch (aggregators.getType(i)) {
        case DOUBLE:
          doubleValues[i] = new double[maxVectorSize];
          valuess[i] = doubleValues[i];
          break;
        case FLOAT:
          floatValues[i] = new float[maxVectorSize];
          valuess[i] = floatValues[i];
          break;
        case LONG:
          longValues[i] = new long[maxVectorSize];
          valuess[i] = longValues[i];
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    int curVectorSize = 0;
    for (; curVectorSize < maxVectorSize && baseIterator.hasNext(); curVectorSize++) {
      final int bucket = baseIterator.nextInt();
      final int bucketPosition = hashTable.bucketMemoryPosition(bucket);

      // TODO: which is better between making a copy and indirect read?
      hashTable.memory().copyTo(
          bucketPosition + hashTable.bucketKeyOffset(),
          keyVector,
          curVectorSize * hashTable.keySize(),
          hashTable.keySize()
      );

      final int aggregatorsOffset = bucketPosition + hashTable.bucketValueOffset();
      for (int i = 0; i < aggregators.size(); i++) {
        switch (aggregators.getType(i)) {
          case DOUBLE:
            doubleValues[i][curVectorSize] = aggregators.getDouble(hashTable.memory(), aggregatorsOffset, i);
            break;
          case FLOAT:
            floatValues[i][curVectorSize] = aggregators.getFloat(hashTable.memory(), aggregatorsOffset, i);
            break;
          case LONG:
            longValues[i][curVectorSize] = aggregators.getLong(hashTable.memory(), aggregatorsOffset, i);
            break;
          default:
            throw new UnsupportedOperationException();
        }
      }
//                    System.err.println(Thread.currentThread() + ", row: " + Groupers.deserializeToRow(segmentId, keyMemory, values));
    }

//                  StringBuilder keys = new StringBuilder(Thread.currentThread() + ", keys: ");
//                  for (int i = 0; i < curVectorSize; i++) {
//                    keys.append(keyVector.getInt(i * keySize)).append(", ");
//                    keys.append(keyVector.getInt(i * keySize + 4)).append(", ");
//                  }
//
//                  System.err.println(keys);

    return new MemoryVectorEntry(nextVectorId++, keyVector, valuess, maxVectorSize, curVectorSize, segmentId);
  }

  @Override
  public int size()
  {
    return hashTable.size();
  }
}
