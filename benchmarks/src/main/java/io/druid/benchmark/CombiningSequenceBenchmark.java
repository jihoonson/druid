/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulators;
import io.druid.java.util.common.guava.ParallelMergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
public class CombiningSequenceBenchmark
{
  // Number of Sequences to Merge
  @Param({"8"})
  int count;

  // Number of elements in each sequence
  @Param({"1000000"})
  int sequenceLength;

  private List<Sequence<Pair<Integer, Integer>>> sequences;
  private ExecutorService exec;

  private static final BinaryFn<Pair<Integer, Integer>, Pair<Integer, Integer>, Pair<Integer, Integer>> mergeFn = (pair1, pair2) -> {
    if (pair1 == null) {
      return pair2;
    }

    if (pair2 == null) {
      return pair1;
    }

    return Pair.of(pair1.lhs, pair1.rhs + pair2.rhs);
  };

  private static final Ordering<Pair<Integer, Integer>> ordering = Ordering.natural().onResultOf(Pair.lhsFn());

  @Setup
  public void setup()
  {
    Random rand = new Random(0);
    sequences = Lists.newArrayList();

    for (int i = 0; i < count; i++) {
      sequences.add(
          Sequences.simple(
              IntStream.range(0, sequenceLength)
                       .mapToObj(unused -> new Pair<>(rand.nextInt(10000), rand.nextInt()))
                       .sorted(Comparator.comparingInt(p -> p.lhs))
                       .collect(Collectors.toList())
          ).combine(ordering, mergeFn)
      );
    }

    exec = Executors.newFixedThreadPool(8);
  }

  @TearDown
  public void teardown()
  {
    if (exec != null) {
      exec.shutdownNow();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void combineSequential(final Blackhole blackhole)
  {
    List<Pair<Integer, Integer>> accumulate = Sequences.merge(
        Sequences.simple(sequences),
        ordering
    ).combine(ordering, mergeFn).accumulate(
        new ArrayList<>(),
        Accumulators.list()
    );
    blackhole.consume(accumulate);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void combineParallel(final Blackhole blackhole)
  {
    List<Pair<Integer, Integer>> accumulate = new ParallelMergeSequence<>(
        exec,
        sequences,
        ordering
    ).combine(ordering, mergeFn).accumulate(
        new ArrayList<>(),
        Accumulators.list()
    );
    blackhole.consume(accumulate);
  }
}
