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
package org.apache.druid.indexing.overlord;

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LockRequestForNewSegment implements LockRequest
{
  private final LockGranularity lockGranularity;
  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final ShardSpecFactory shardSpecFactory;
  private final int priority;
  private final int numNewSegments;
  private final String baseSequenceName;
  @Nullable
  private final String previsousSegmentId;
  private final Set<Integer> overshadowingSegments;
  private final boolean firstPartition;
  private final boolean skipSegmentLineageCheck;

  public LockRequestForNewSegment(
      LockGranularity lockGranularity,
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      ShardSpecFactory shardSpecFactory,
      int priority,
      int numNewSegments,
      String baseSequenceName,
      @Nullable String previsousSegmentId,
      Set<Integer> overshadowingSegments,
      boolean firstPartition,
      boolean skipSegmentLineageCheck
  )
  {
    this.lockGranularity = lockGranularity;
    this.lockType = lockType;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.shardSpecFactory = shardSpecFactory;
    this.priority = priority;
    this.numNewSegments = numNewSegments;
    this.baseSequenceName = baseSequenceName;
    this.previsousSegmentId = previsousSegmentId;
    this.overshadowingSegments = overshadowingSegments;
    this.firstPartition = firstPartition;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
  }

  @Override
  public LockGranularity getGranularity()
  {
    return lockGranularity;
  }

  @Override
  public TaskLockType getType()
  {
    return lockType;
  }

  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  public ShardSpecFactory getShardSpecFactory()
  {
    return shardSpecFactory;
  }

  @Override
  public String getVersion()
  {
    return DateTimes.nowUtc().toString();
  }

  @Override
  public boolean isRevoked()
  {
    return false;
  }

  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @Nullable
  public String getPrevisousSegmentId()
  {
    return previsousSegmentId;
  }

  public int getNumNewSegments()
  {
    return numNewSegments;
  }

  public Set<Integer> getOvershadowingSegments()
  {
    return overshadowingSegments;
  }

  public boolean isFirstPartition()
  {
    return firstPartition;
  }

  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  public TaskLock toLock(List<SegmentIdentifier> newSegmentIds)
  {
    final String version = newSegmentIds.get(0).getVersion();
    Preconditions.checkState(
        newSegmentIds.stream().allMatch(id -> id.getVersion().equals(version)),
        "WTH? new segmentIds have different version? [%s]",
        newSegmentIds
    );

    switch (lockGranularity) {
      case TIME_CHUNK:
        return new TimeChunkLock(
            lockType,
            groupId,
            dataSource,
            interval,
            version,
            priority
        );
      case SEGMENT:
        return new SegmentLock(
            lockType,
            groupId,
            dataSource,
            interval,
            newSegmentIds.stream().map(id -> id.getShardSpec().getPartitionNum()).collect(Collectors.toSet()),
            version,
            priority
        );
      default:
        throw new ISE("Unknown lockGranularity[%s]", lockGranularity);
    }
  }

  @Override
  public String toString()
  {
    return "LockRequestForNewSegment{" +
           "lockGranularity=" + lockGranularity +
           ", lockType=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", shardSpecFactory=" + shardSpecFactory +
           ", priority=" + priority +
           ", numNewSegments=" + numNewSegments +
           ", baseSequenceName='" + baseSequenceName + '\'' +
           ", previsousSegmentId='" + previsousSegmentId + '\'' +
           ", overshadowingSegments=" + overshadowingSegments +
           ", firstPartition=" + firstPartition +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           '}';
  }
}
