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
package org.apache.druid.java.util.common.concurrent;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.query.ThreadResource;
import org.skife.jdbi.v2.Query;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReservingThreadPool implements Closeable
{
  private static final int NUM_CURRENT_AVAILABLE_THREADS = -1;

  private final ExecutorService delegate;
  private final BlockingPool<ThreadResource> resourcePool;

  public ReservingThreadPool(int poolSize, String nameFormat)
  {
    this.delegate = Execs.multiThreaded(poolSize, nameFormat);
    this.resourcePool = new DefaultBlockingPool<>(ThreadResource::new, poolSize);
  }

  @Nullable
  <T> ReservedThreadPoolExecutor reserve(Query<T> query, int n)
  {
//      final boolean hasTimeout = QueryContexts.hasTimeout(query);
//      final long timeout = QueryContexts.getTimeout(query);

    if (n == NUM_CURRENT_AVAILABLE_THREADS) {
      final List<ReferenceCountingResourceHolder<ThreadResource>> availableResources = resourcePool.pollAll();
      return new ReservedThreadPoolExecutor(availableResources, availableResources.size());
    } else {
      return new ReservedThreadPoolExecutor(resourcePool.takeBatch(n), resourcePool.available());
    }
  }

  @Override
  public void close() throws IOException
  {
    delegate.shutdownNow();
  }

  class ReservedThreadPoolExecutor implements ExecutorService
  {
    private final List<ReferenceCountingResourceHolder<ThreadResource>> resources;
    private final int numAvailableResources;

    private ReservedThreadPoolExecutor(List<ReferenceCountingResourceHolder<ThreadResource>> resources, int numAvailableResources)
    {
      this.resources = resources;
      this.numAvailableResources = numAvailableResources;
    }

    @Override
    public void shutdown()
    {
      resources.forEach(ReferenceCountingResourceHolder::close);
    }

    @Override
    public List<Runnable> shutdownNow()
    {
      return null;
    }

    @Override
    public boolean isShutdown()
    {
      return false;
    }

    @Override
    public boolean isTerminated()
    {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
      return false;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
      return null;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
      return null;
    }

    @Override
    public Future<?> submit(Runnable task)
    {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit
    ) throws InterruptedException
    {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
      return null;
    }

    @Override
    public void execute(Runnable command)
    {

    }
  }
}
