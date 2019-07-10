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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC", "-agentpath:/Applications/YourKit-Java-Profiler-2019.1.app/Contents/Resources/bin/mac/libyjpagent.jnilib=sampling,onexit=memory"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
public class VersionedIntervalTimelineBenchmark
{
  private static final String DATA_SOURCE = "dataSource";
  private static final Interval TOTAL_INTERVAL = Intervals.of("2018/2019");
  private static final double NEW_ROOT_GEN_SEGMENTS_RATIO_AFTER_COMPACTION = 0.1;

  @Param({"10", "100", "1000"})
  private int numInitialRootGenSegmentsPerInterval;

  @Param({"1", "5"})
  private int numNonRootGenerations;

  @Param({"0.0", "0.5"})
  private double numCompactedSegmentsRatioToNumInitialSegments;

  @Param({"MONTH", "DAY"})
  private GranularityType segmentGranularity;

  private List<Interval> intervals;
  private List<DataSegment> segments;
  private VersionedIntervalTimeline<String, DataSegment> timeline;
  private List<DataSegment> newSegments;

  @Setup
  public void setup()
  {
    intervals = Lists.newArrayList(segmentGranularity.getDefaultGranularity().getIterable(TOTAL_INTERVAL));
    segments = new ArrayList<>(intervals.size() * numInitialRootGenSegmentsPerInterval);
    Map<Interval, Integer> nextRootGenPartitionIds = new HashMap<>(intervals.size());
    Map<Interval, Integer> nextNonRootGenPartitionIds = new HashMap<>(intervals.size());
    Map<Interval, Short> nextMinorVersions = new HashMap<>(intervals.size());

    final String majorVersion = DateTimes.nowUtc().toString();

    for (Interval interval : intervals) {
      // Generate root generation segments
      int nextRootGenPartitionId = 0;
      int nextNonRootGenPartitionId = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID;
      for (int i = 0; i < numInitialRootGenSegmentsPerInterval; i++) {
        segments.add(newSegment(interval, majorVersion, new NumberedShardSpec(nextRootGenPartitionId++, 0)));
      }

      final int numNewRootGenSegmentsAfterCompaction =
          (int) (numInitialRootGenSegmentsPerInterval * NEW_ROOT_GEN_SEGMENTS_RATIO_AFTER_COMPACTION);
      final int numCompactedSegments =
          (int) (numInitialRootGenSegmentsPerInterval * numCompactedSegmentsRatioToNumInitialSegments);
      for (int i = 0; i < numNonRootGenerations; i++) {
        // Compacted segments
        for (int j = 0; j < numCompactedSegments; j++) {
          segments.add(
              newSegment(
                  interval,
                  majorVersion,
                  new NumberedOverwriteShardSpec(
                      nextNonRootGenPartitionId++,
                      0,
                      nextRootGenPartitionId,
                      (short) (i + 1),
                      (short) numCompactedSegments
                  )
              )
          );
        }

        // New segments
        for (int j = 0; j < numNewRootGenSegmentsAfterCompaction; j++) {
          segments.add(newSegment(interval, majorVersion, new NumberedShardSpec(nextRootGenPartitionId++, 0)));
        }
      }
      nextRootGenPartitionIds.put(interval, nextRootGenPartitionId);
      nextNonRootGenPartitionIds.put(interval, nextNonRootGenPartitionId);
      nextMinorVersions.put(interval, (short) (numNonRootGenerations + 1));
    }

    timeline = VersionedIntervalTimeline.forSegments(segments);

    newSegments = new ArrayList<>(200);
    for (int i = 0; i < 100; i++) {
      final Interval interval = intervals.get(ThreadLocalRandom.current().nextInt(intervals.size()));
      final int rootPartitionId = nextRootGenPartitionIds.get(interval);
      final int nonRootPartitionId = nextNonRootGenPartitionIds.get(interval);
      final short minorVersion = nextMinorVersions.get(interval);
      newSegments.add(
          newSegment(
              interval,
              majorVersion,
              new NumberedShardSpec(rootPartitionId, 0)
          )
      );
      newSegments.add(
          newSegment(
              interval,
              majorVersion,
              new NumberedOverwriteShardSpec(
                  nonRootPartitionId,
                  0,
                  rootPartitionId,
                  minorVersion,
                  (short) 1
              )
          )
      );
      nextRootGenPartitionIds.put(interval, rootPartitionId + 1);
      nextNonRootGenPartitionIds.put(interval, nonRootPartitionId + 1);
      nextMinorVersions.put(interval, (short) (minorVersion + 1));
    }
  }

  @Benchmark
  public void testAddAndRemove(Blackhole blackhole)
  {
    for (DataSegment newSegment : newSegments) {
      timeline.add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
    for (DataSegment newSegment : newSegments) {
      timeline.remove(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
  }

  @Benchmark
  public void testLookup(Blackhole blackhole)
  {
    for (int i = 0; i < 100; i++) {
      final int intervalIndex = ThreadLocalRandom.current().nextInt(intervals.size() - 2);
      final Interval queryInterval = new Interval(
          intervals.get(intervalIndex).getStart(),
          intervals.get(intervalIndex + 2).getEnd()
      );
      blackhole.consume(timeline.lookup(queryInterval));
    }
  }

  @Benchmark
  public void testIsOvershadowed(Blackhole blackhole)
  {
    for (int i = 0; i < 100; i++) {
      final DataSegment segment = segments.get(ThreadLocalRandom.current().nextInt(segments.size()));
      blackhole.consume(timeline.isOvershadowed(segment.getInterval(), segment.getVersion(), segment));
    }
  }

  @Benchmark
  public void testFindFullyOvershadowed(Blackhole blackhole)
  {
    blackhole.consume(timeline.findFullyOvershadowed());
  }

  private static DataSegment newSegment(Interval interval, String version, ShardSpec shardSpec)
  {
    return new DataSegment(
        DATA_SOURCE,
        interval,
        version,
        null,
        null,
        null,
        shardSpec,
        9,
        10
    );
  }
}
