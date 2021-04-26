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

package org.apache.druid.query;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.ISE;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: name
public class QueryBlockingMemoryPool implements Closeable
{
  private final BlockingPool<ByteBuffer> backedPool;
  private final ReferenceCountingResourceHolder<ByteBuffer> reserved;
  private final AtomicBoolean isReservedAvailable = new AtomicBoolean(true);

  public QueryBlockingMemoryPool(
      BlockingPool<ByteBuffer> backedPool,
      ReferenceCountingResourceHolder<ByteBuffer> reserved
  )
  {
    this.backedPool = backedPool;
    this.reserved = reserved;
  }

  public ReferenceCountingResourceHolder<ByteBuffer> take()
  {
    if (isReservedAvailable.compareAndSet(true, false)) {
      final AtomicBoolean isClosed = new AtomicBoolean(false);
      return new ReferenceCountingResourceHolder<>(
          reserved.get(),
          () -> {
            if (isClosed.compareAndSet(false, true)) {
              if (!isReservedAvailable.compareAndSet(false, true)) {
                throw new ISE("WTH?");
              }
            }
          }
      );
    }

    return backedPool.takeBatch(1).get(0);
  }

  public Optional<ReferenceCountingResourceHolder<ByteBuffer>> take(long timeoutMs)
  {
    // TODO: should i wait for some resources to be available in reservedPool?
    if (isReservedAvailable.compareAndSet(true, false)) {
      final AtomicBoolean isClosed = new AtomicBoolean(false);
      return Optional.of(
          new ReferenceCountingResourceHolder<>(
              reserved.get(),
              () -> {
                if (isClosed.compareAndSet(false, true)) {
                  if (!isReservedAvailable.compareAndSet(false, true)) {
                    throw new ISE("WTH?");
                  }
                }
              }
          )
      );
    }

    List<ReferenceCountingResourceHolder<ByteBuffer>> holders = backedPool.takeBatch(1, timeoutMs);
    return holders.isEmpty() ? Optional.empty() : Optional.of(holders.get(0));
  }

  @Override
  public void close() throws IOException
  {
    reserved.close();
  }
}
