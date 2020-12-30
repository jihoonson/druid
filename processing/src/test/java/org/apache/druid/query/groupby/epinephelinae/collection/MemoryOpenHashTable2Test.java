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

import org.apache.datasketches.memory.WritableMemory;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MemoryOpenHashTable2Test
{

  @Test
  public void test()
  {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(12416);
    final int keySize = 8; // 2 ints
    final int valueSize = 4; // count
    final int numBuckets = 1024;
    final MemoryOpenHashTable2 hashTable = new MemoryOpenHashTable2(
        WritableMemory.wrap(buffer, ByteOrder.nativeOrder()),
        numBuckets,
        (int) (numBuckets * 0.75),
        keySize,
        valueSize
    );

    final byte[] keyBuffer = new byte[keySize];
    WritableMemory keyMemory = WritableMemory.wrap(keyBuffer);
    keyMemory.putInt(0, 10);
    keyMemory.putInt(4, 20);
    int bucket = hashTable.findBucket(keyMemory.hashCode(), keyMemory, 0);
    System.out.println(bucket);
    bucket = -bucket - 1;
    hashTable.initBucket(bucket, keyMemory, 0);

    bucket = hashTable.findBucket(keyMemory.hashCode(), keyMemory, 0);
    System.out.println(bucket);
  }
}