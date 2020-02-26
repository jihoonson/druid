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

package org.apache.druid;

import com.google.common.collect.FluentIterable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode({Mode.Throughput})
public class StreamBenchmark
{
  private static final int NUM_INTEGERS = 10000;
  private static final int NUM_SETS = 100;

  private IntegersSet integersSet;

  @Setup
  public void setup()
  {
    integersSet = new IntegersSet();
    for (int i = 0; i < NUM_SETS; i++) {
      Integers integers = new Integers();
      for (int j = 0; j < NUM_INTEGERS; j++) {
        integers.add(ThreadLocalRandom.current().nextInt(j + 1));
      }
      integersSet.add(integers);
    }
  }

  @Benchmark
  public void flatMapStream(Blackhole blackhole)
  {
    integersSet.stream().flatMap(Collection::stream).forEach(blackhole::consume);
  }

  @Benchmark
  public void flatMapIterator(Blackhole blackhole)
  {
    FluentIterable.from(integersSet).transformAndConcat(integers -> integers).forEach(blackhole::consume);
  }

  @Benchmark
  public void sumStream(Blackhole blackhole)
  {
    blackhole.consume(integersSet.stream().flatMap(Collection::stream).mapToInt(i -> i).sum());
  }

  @Benchmark
  public void sumIterator(Blackhole blackhole)
  {
    int sum = 0;
    for (Integer i : FluentIterable.from(integersSet).transformAndConcat(is -> is)) {
      sum += i;
    }
    blackhole.consume(sum);
  }

  @Benchmark
  public void streamToList(Blackhole blackhole)
  {
    blackhole.consume(integersSet.stream().flatMap(Collection::stream).collect(Collectors.toList()));
  }

  @Benchmark
  public void fluentIteratorToList(Blackhole blackhole)
  {
    blackhole.consume(FluentIterable.from(integersSet).transformAndConcat(integers -> integers).toList());
  }

  private static class Integers extends ArrayList<Integer>
  {
  }

  private static class IntegersSet extends HashSet<Integers>
  {
  }
}
