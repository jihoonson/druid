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
package org.apache.druid.timeline;

import it.unimi.dsi.fastutil.ints.AbstractIntCollection;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataSegmentTest2
{
  private static final String VERSION = DateTimes.nowUtc().toString();

  @Test
  public void testTest() throws IOException
  {
    DateTime cur = DateTimes.nowUtc();
    final List<DataSegment2> segment2s = new ArrayList<>(5_000_000);
    for (int i = 0; i < 1_000_000; i++) {
      cur.plusDays(1);
      for (int j = 0; j < 5 ; j++) {
        segment2s.add(
            segment(
                new Interval(cur, cur.plusDays(1)),
                j,
//                IntStream.range(20, 30).boxed().collect(Collectors.toSet()),
//                IntStream.range(0, 20).boxed().collect(Collectors.toSet()),
//                IntStream.range(0, 5).boxed().collect(Collectors.toSet())
//                IntStream.range(20, 20).boxed().collect(Collectors.toSet()),
//                IntStream.range(0, 0).boxed().collect(Collectors.toSet()),
//                IntStream.range(0, 0).boxed().collect(Collectors.toSet())
                null,
                null,
                null
            )
        );
      }
    }

    System.out.println(segment2s.size());
    System.in.read();
  }

  private static DataSegment2 segment(
      Interval interval,
      int partitionNum,
      Set<Integer> overshadowedSegments,
      Set<Integer> indirectOvershadowedSegments,
      Set<Integer> atomicUpdateGroup
  )
  {
    return new DataSegment2(
        "dataSource",
        interval,
        VERSION,
        new NumberedShardSpec(partitionNum, 0),
        9,
        100L,
        overshadowedSegments,
        indirectOvershadowedSegments,
        atomicUpdateGroup
    );
  }

  private static class DataSegment2
  {
    private final DataSegment dataSegment;
//    private final Set<Integer> indirectOvershadowedSegments;
//    private final IntSet indirectOvershadowedSegments;
    private final BitSet indirectOvershadowedSegments;

    private DataSegment2(
        String dataSource,
        Interval interval,
        String version,
        ShardSpec shardSpec,
        Integer binaryVersion,
        long size,
        Set<Integer> overshadowedSegments,
        Set<Integer> indirectOvershadowedSegments,
        Set<Integer> atomicUpdateGroup
    )
    {
      this.dataSegment = new DataSegment(
          dataSource,
          interval,
          version,
          null,
          null,
          null,
          shardSpec,
          binaryVersion,
          size,
          overshadowedSegments,
          atomicUpdateGroup
      );
//      this.indirectOvershadowedSegments = indirectOvershadowedSegments;

//      this.indirectOvershadowedSegments = indirectOvershadowedSegments
//          .stream()
//          .mapToInt(i -> i)
//          .collect(IntArraySet::new, IntArraySet::add, AbstractIntCollection::addAll);

      if (indirectOvershadowedSegments != null) {
        this.indirectOvershadowedSegments = new BitSet(indirectOvershadowedSegments.size());
        for (Integer i : indirectOvershadowedSegments) {
          this.indirectOvershadowedSegments.set(i);
        }
      } else {
        this.indirectOvershadowedSegments = null;
      }
    }
  }
}
