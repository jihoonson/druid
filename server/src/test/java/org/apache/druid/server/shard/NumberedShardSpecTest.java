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

package org.apache.druid.server.shard;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.ServerTestHelper;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NumberedShardSpecTest
{
  @Test
  public void testSerdeRoundTrip() throws Exception
  {
    final ShardSpec spec = ServerTestHelper.MAPPER.readValue(
        ServerTestHelper.MAPPER.writeValueAsBytes(new NumberedShardSpec(1, 2)),
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testSerdeBackwardsCompat() throws Exception
  {
    final ShardSpec spec = ServerTestHelper.MAPPER.readValue(
        "{\"type\": \"numbered\", \"partitions\": 2, \"partitionNum\": 1}",
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.of(
        new NumberedShardSpec(0, 3),
        new NumberedShardSpec(1, 3),
        new NumberedShardSpec(2, 3)
    );

    final List<PartitionChunk<String>> chunks = Lists.transform(
        specs,
        new Function<ShardSpec, PartitionChunk<String>>()
        {
          @Override
          public PartitionChunk<String> apply(ShardSpec shardSpec)
          {
            return shardSpec.createChunk("rofl");
          }
        }
    );

    Assert.assertEquals(0, chunks.get(0).getChunkNumber());
    Assert.assertEquals(1, chunks.get(1).getChunkNumber());
    Assert.assertEquals(2, chunks.get(2).getChunkNumber());

    Assert.assertTrue(chunks.get(0).isStart());
    Assert.assertFalse(chunks.get(1).isStart());
    Assert.assertFalse(chunks.get(2).isStart());

    Assert.assertFalse(chunks.get(0).isEnd());
    Assert.assertFalse(chunks.get(1).isEnd());
    Assert.assertTrue(chunks.get(2).isEnd());

    Assert.assertTrue(chunks.get(0).abuts(chunks.get(1)));
    Assert.assertTrue(chunks.get(1).abuts(chunks.get(2)));

    Assert.assertFalse(chunks.get(0).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(0).abuts(chunks.get(2)));
    Assert.assertFalse(chunks.get(1).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(1).abuts(chunks.get(1)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(1)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(2)));
  }

  @Test
  public void testVersionedIntervalTimelineBehaviorForNumberedShardSpec()
  {
    //core partition chunks
    PartitionChunk<OvershadowableString> chunk0 = new NumberedShardSpec(0, 2)
        .createChunk(new OvershadowableString("0", 0));
    PartitionChunk<OvershadowableString> chunk1 = new NumberedShardSpec(1, 2)
        .createChunk(new OvershadowableString("1", 1));

    //appended partition chunk
    PartitionChunk<OvershadowableString> chunk4 = new NumberedShardSpec(4, 2)
        .createChunk(new OvershadowableString("4", 4));

    //incomplete partition sets
    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk0),
        Collections.EMPTY_SET
    );

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk1),
        Collections.EMPTY_SET
    );

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk4),
        Collections.EMPTY_SET
    );


    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk0, chunk4),
        Collections.EMPTY_SET
    );


    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk1, chunk4),
        Collections.EMPTY_SET
    );

    //complete partition sets
    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk1, chunk0),
        ImmutableSet.of(new OvershadowableString("0", 0), new OvershadowableString("1", 0))
    );

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk4, chunk1, chunk0),
        ImmutableSet.of(
            new OvershadowableString("0", 0),
            new OvershadowableString("1", 1),
            new OvershadowableString("4", 4)
        )
    );

    // a partition set with 0 core partitions
    chunk0 = new NumberedShardSpec(0, 0).createChunk(new OvershadowableString("0", 0));
    chunk4 = new NumberedShardSpec(4, 0).createChunk(new OvershadowableString("4", 4));

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk0),
        ImmutableSet.of(new OvershadowableString("0", 0))
    );

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk4),
        ImmutableSet.of(new OvershadowableString("4", 4))
    );

    testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
        ImmutableList.of(chunk4, chunk0),
        ImmutableSet.of(new OvershadowableString("0", 0), new OvershadowableString("4", 4))
    );
  }

  private void testVersionedIntervalTimelineBehaviorForNumberedShardSpec(
      List<PartitionChunk<OvershadowableString>> chunks,
      Set<OvershadowableString> expectedObjects
  )
  {
    VersionedIntervalTimeline<String, OvershadowableString> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    Interval interval = Intervals.of("2000/3000");
    String version = "v1";
    for (PartitionChunk<OvershadowableString> chunk : chunks) {
      timeline.add(interval, version, chunk);
    }

    Set<OvershadowableString> actualObjects = new HashSet<>();
    List<TimelineObjectHolder<String, OvershadowableString>> entries = timeline.lookup(interval);
    for (TimelineObjectHolder<String, OvershadowableString> entry : entries) {
      for (PartitionChunk<OvershadowableString> chunk : entry.getObject()) {
        actualObjects.add(chunk.getObject());
      }
    }
    Assert.assertEquals(expectedObjects, actualObjects);
  }

  private static final class OvershadowableString implements Overshadowable<OvershadowableString>
  {
    private final int partitionId;
    private final String val;

    OvershadowableString(String val, int partitionId)
    {
      this.val = val;
      this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OvershadowableString that = (OvershadowableString) o;
      return partitionId == that.partitionId &&
             Objects.equals(val, that.val);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(partitionId, val);
    }

    @Override
    public boolean isOvershadow(OvershadowableString other)
    {
      return false;
    }

    @Override
    public int getStartRootPartitionId()
    {
      return partitionId;
    }

    @Override
    public int getEndRootPartitionId()
    {
      return partitionId + 1;
    }

    @Override
    public short getMinorVersion()
    {
      return 0;
    }

    @Override
    public short getAtomicUpdateGroupSize()
    {
      return 1;
    }
  }
}
