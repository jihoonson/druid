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

package io.druid.java.util.common.guava;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.nary.BinaryFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelMergeSequenceTest
{
  @Test
  public void testAccumulate() throws Exception
  {
    final ExecutorService service = Executors.newFixedThreadPool(4);

    List<Pair<Integer, Integer>> pairs1 = ImmutableList.of(
        Pair.of(0, 1),
        Pair.of(0, 2),
        Pair.of(0, 3),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 1),
        Pair.of(5, 10),
        Pair.of(6, 1)
    );

    List<Pair<Integer, Integer>> pairs2 = ImmutableList.of(
        Pair.of(0, 1),
        Pair.of(0, 2),
        Pair.of(2, 1),
        Pair.of(5, 1),
        Pair.of(5, 1)
    );

    List<Pair<Integer, Integer>> pairs3 = ImmutableList.of(
        Pair.of(0, 1),
        Pair.of(0, 2),
        Pair.of(1, 1),
        Pair.of(6, 1)
    );

    List<Pair<Integer, Integer>> pairs4 = ImmutableList.of(
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 1),
        Pair.of(5, 10),
        Pair.of(6, 1)
    );

    final Ordering<Pair<Integer, Integer>> ordering = Ordering.natural().onResultOf(Pair.<Integer, Integer>lhsFn());
    final BinaryFn<Pair<Integer, Integer>, Pair<Integer, Integer>, Pair<Integer, Integer>> mergeFn = (pair1, pair2) -> {
      if (pair1 == null) {
        return pair2;
      }

      if (pair2 == null) {
        return pair1;
      }

      return Pair.of(pair1.lhs, pair1.rhs + pair2.rhs);
    };

    final ParallelMergeSequence<Pair<Integer, Integer>> sequence = new ParallelMergeSequence<>(
        service,
        ordering,
        Sequences.simple(
            ImmutableList.of(
                Sequences.combine(Sequences.simple(pairs1), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs2), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs3), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs4), ordering, mergeFn)
            )
        )
    );

    final List<Pair<Integer, Integer>> actualResult = Sequences.combine(sequence, ordering, mergeFn).accumulate(
        new ArrayList<>(),
        Accumulators.list()
    );

    final List<Pair<Integer, Integer>> expectedResult = Sequences.merge(
        ordering,
        Sequences.simple(
            ImmutableList.of(
                Sequences.combine(Sequences.simple(pairs1), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs2), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs3), ordering, mergeFn),
                Sequences.combine(Sequences.simple(pairs4), ordering, mergeFn)
            )
        )
    ).combine(ordering, mergeFn).accumulate(new ArrayList<>(), Accumulators.list());

    Assert.assertEquals(expectedResult, actualResult);

    service.shutdownNow();
  }

}
