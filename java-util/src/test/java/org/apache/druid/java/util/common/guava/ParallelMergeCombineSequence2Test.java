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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ParallelMergeCombineSequence2Test
{
  private ExecutorService service;

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(2, "parallel-merge-combine-sequence-test");
  }

  @After
  public void teardown()
  {
    service.shutdown();
  }

  @Test
  public void test()
  {
    List<IntPair> pairs1 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 1)
    );

    List<IntPair> pairs2 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(6, 2),
        new IntPair(10, 2)
    );

    List<IntPair> pairs3 = Arrays.asList(
        new IntPair(4, 5),
        new IntPair(10, 3)
    );

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));
    input.add(Sequences.simple(pairs3));

    final Ordering<IntPair> ordering = Ordering.natural().onResultOf(p -> p.lhs);
    final BinaryFn<IntPair, IntPair, IntPair> mergeFn = (lhs, rhs) -> {
      if (lhs == null) {
        return rhs;
      }

      if (rhs == null) {
        return lhs;
      }

      return new IntPair(lhs.lhs, lhs.rhs + rhs.rhs);
    };

    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.simple(input)),
        ordering,
        mergeFn
    );
    
    final ParallelMergeCombineSequence2<IntPair> parallelMergeCombineSequence2 = new ParallelMergeCombineSequence2<>(
        service,
        input,
        ordering,
        mergeFn,
        2
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence2);

    while (!combiningYielder.isDone() && !parallelMergeCombineYielder.isDone()) {
      System.out.println("combine: " + combiningYielder.get() + ", parallelCombine: " + parallelMergeCombineYielder.get());
      Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
      combiningYielder = combiningYielder.next(combiningYielder.get());
      parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
    }

    Assert.assertTrue(combiningYielder.isDone());
    Assert.assertTrue(parallelMergeCombineYielder.isDone());
  }
  
  private static class IntPair extends Pair<Integer, Integer>
  {
    public IntPair(@Nullable Integer lhs, @Nullable Integer rhs)
    {
      super(lhs, rhs);
    }
  }
}