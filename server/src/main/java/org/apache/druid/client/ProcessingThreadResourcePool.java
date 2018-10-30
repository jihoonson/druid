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
package org.apache.druid.client;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.ThreadResource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool to coordinate callers which contend with each other to get thread resources. This class just provides a
 * logical coordination and the real thread pool should be maintained separately.
 * This can be improved to consider query priority in the future.
 */
class ProcessingThreadResourcePool
{
  private final ReentrantLock lock = new ReentrantLock();
  private final BlockingPool<ThreadResource> resourcePool;

  ProcessingThreadResourcePool(int poolSize)
  {
    this.resourcePool = new DefaultBlockingPool<>(ThreadResource::new, poolSize);
  }

  <T> ReserveResult reserve(Query<T> query, int n) throws InterruptedException
  {
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeout = QueryContexts.getTimeout(query);

    lock.lockInterruptibly();

    try {
      if (n == QueryContexts.NUM_CURRENT_AVAILABLE_THREADS) {
        final int availableResources = resourcePool.available();
        if (availableResources > 1) {
          final List<ReferenceCountingResourceHolder<ThreadResource>> reserved = hasTimeout
                 ? resourcePool.takeBatch(availableResources, timeout)
                 : resourcePool.takeBatch(availableResources);
          return new ReserveResult(reserved, availableResources);
        } else {
          return new ReserveResult(Collections.emptyList(), 1);
        }
      } else {
        return new ReserveResult(
            hasTimeout ? resourcePool.takeBatch(n, timeout) : resourcePool.takeBatch(n),
            resourcePool.available()
        );
      }
    }
    finally {
      lock.unlock();
    }
  }

  static class ReserveResult
  {
    private final List<ReferenceCountingResourceHolder<ThreadResource>> resources;
    private final int numAvailableResources;

    private ReserveResult(List<ReferenceCountingResourceHolder<ThreadResource>> resources, int numAvailableResources)
    {
      this.resources = resources;
      this.numAvailableResources = numAvailableResources;
    }

    boolean isOk()
    {
      return !resources.isEmpty();
    }

    List<ReferenceCountingResourceHolder<ThreadResource>> getResources()
    {
      return resources;
    }

    int getNumAvailableResources()
    {
      return numAvailableResources;
    }
  }
}
