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

package org.apache.druid.query.groupby.epinephelinae.vector;

import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.groupby.epinephelinae.FixedSizeHashVectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.Grouper.MemoryVectorEntry;
import org.apache.druid.query.groupby.epinephelinae.SizedIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * TODO: more javadoc
 *
 * Iterates one bucketed hash table.
 * Maybe i don't need this. or maybe need to clean up resources
 */
public class PartitionedHashTableIterator implements SizedIterator<MemoryVectorEntry>, Closeable
{
  private final int bucket;
  private final ReferenceCountingResourceHolder<ByteBuffer> currentBufferHolder;
  private final FixedSizeHashVectorGrouper vectorGrouper;
  private final SizedIterator<MemoryVectorEntry> entryIterator;

  public PartitionedHashTableIterator(
      int bucket,
      ReferenceCountingResourceHolder<ByteBuffer> currentBufferHolder,
      FixedSizeHashVectorGrouper vectorGrouper,
      SizedIterator<MemoryVectorEntry> entryIterator
  )
  {
    this.bucket = bucket;
    this.currentBufferHolder = currentBufferHolder;
    this.vectorGrouper = vectorGrouper;
    this.entryIterator = entryIterator;

    currentBufferHolder.increment();
  }

  public int getBucket()
  {
    return bucket;
  }

  @Override
  public int size()
  {
    return entryIterator.size();
  }

  @Override
  public boolean hasNext()
  {
    return entryIterator.hasNext();
  }

  @Override
  public MemoryVectorEntry next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return entryIterator.next();
  }

  @Override
  public void close() throws IOException
  {
    Closer closer = Closer.create();
//    closer.register(vectorGrouper); // TODO: should not close grouper here. should be closed when timestampdIterators is closed
    closer.register(currentBufferHolder);
    closer.close();
  }
}
