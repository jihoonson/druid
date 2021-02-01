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

package org.apache.druid.benchmark.query;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.groupby.epinephelinae.GroupByShuffleMergingQueryRunner.ProcessingCallablePool;
import org.apache.druid.query.groupby.epinephelinae.GroupByShuffleMergingQueryRunner.ProcessingTask;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ProcessingCallableSchedulerBenchmark
{
  @Param({"32"})
  private int numTasks;

  @Param({"16"})
  private int numThreads;

  @Param({"10"})
  private long taskRunTimeMs;

  private ExecutorService exec;
  private ProcessingCallablePool scheduler;

  @Setup(Level.Trial)
  public void setup()
  {
    exec = Execs.multiThreaded(numThreads, "processing-callable-scheduler-benchmark-%d");
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    if (scheduler != null) {
      scheduler.shutdown();
    }
    if (exec != null) {
      exec.shutdownNow();
    }
  }

  @Benchmark
  public void fixedThreadPool() throws ExecutionException, InterruptedException
  {
    final List<Future> futures = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      futures.add(exec.submit(() -> {
        Thread.sleep(taskRunTimeMs);
        return null;
      }));
    }
    for (Future f : futures) {
      f.get();
    }
  }

  @Benchmark
  public void processingCallableScheduler() throws ExecutionException, InterruptedException
  {
    if (scheduler == null) {
      scheduler = new ProcessingCallablePool(exec, 1, numThreads);
    }
    final List<Future> futures = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      final SettableFuture<Void> future = SettableFuture.create();
      scheduler.schedule(
          new ProcessingTask<Void>()
          {
            @Override
            public Void run() throws Exception
            {
              Thread.sleep(taskRunTimeMs);
              return null;
            }

            @Override
            public SettableFuture<Void> getResultFuture()
            {
              return future;
            }

            @Override
            public boolean isSentinel()
            {
              return false;
            }
          }
      );
      futures.add(future);
    }
    for (Future f : futures) {
      f.get();
    }
  }
}
