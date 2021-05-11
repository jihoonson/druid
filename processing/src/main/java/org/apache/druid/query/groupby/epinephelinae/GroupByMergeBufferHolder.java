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

import org.apache.druid.collections.ReferenceCountingResourceHolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class GroupByMergeBufferHolder implements Closeable
{
  private final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder;
  private final BlockingQueue<ReferenceCountingResourceHolder<ByteBuffer>> waitQueue = new ArrayBlockingQueue<>(1);

  public GroupByMergeBufferHolder(ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder)
  {
    this.mergeBufferHolder = mergeBufferHolder;
    mergeBufferHolder.increment();
    waitQueue.add(new ReferenceCountingResourceHolder<>(mergeBufferHolder.get(), () -> {}));
  }

  @Nullable
  public ReferenceCountingResourceHolder<ByteBuffer> getBuffer()
  {
    final ReferenceCountingResourceHolder<ByteBuffer> bufferHolder = waitQueue.poll();
    if (bufferHolder == null) {
      return null;
    } else {
      return new ReferenceCountingResourceHolder<>(
          bufferHolder.get(),
          () -> waitQueue.add(bufferHolder)
      );
    }
  }

  // TODO: timeout
  public ReferenceCountingResourceHolder<ByteBuffer> waitBuffer() throws InterruptedException
  {
    final ReferenceCountingResourceHolder<ByteBuffer> bufferHolder = waitQueue.take();
    return new ReferenceCountingResourceHolder<>(
        bufferHolder.get(),
        () -> waitQueue.add(bufferHolder)
    );
  }

  @Override
  public void close() throws IOException
  {
    mergeBufferHolder.close();
  }
}
