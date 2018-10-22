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
package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.overlord.TaskLockbox.LockRequest;
import org.apache.druid.indexing.overlord.TaskLockbox.SegmentLockRequest;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Lock for a single segment. Should be unique for (dataSource, interval, partitionId).
 */
public class SegmentLock implements TaskLock
{
  private final TaskLockType type;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final Set<Integer> partitionIds;
  private final String version;
  private final int priority;
  private final boolean revoked;

  @JsonCreator
  public SegmentLock(
      @JsonProperty("type") TaskLockType type,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("partitionIds") Set<Integer> partitionIds,
      @JsonProperty("version") String version,
      @JsonProperty("priority") int priority,
      @JsonProperty("revoked") boolean revoked
  )
  {
    this.type = Preconditions.checkNotNull(type, "type");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.partitionIds = partitionIds;
    this.version = Preconditions.checkNotNull(version, "version");
    this.priority = priority;
    this.revoked = revoked;
  }

  public SegmentLock(
      TaskLockType type,
      String groupId,
      String dataSource,
      Interval interval,
      Set<Integer> partitionIds,
      String version,
      int priority
  )
  {
    this(type, groupId, dataSource, interval, partitionIds, version, priority, false);
  }

  @Override
  public TaskLock revokedCopy()
  {
    return new SegmentLock(type, groupId, dataSource, interval, partitionIds, version, priority, true);
  }

  @Override
  public TaskLock withPriority(int newPriority)
  {
    return new SegmentLock(type, groupId, dataSource, interval, partitionIds, version, newPriority, revoked);
  }

  @Override
  public LockGranularity getGranularity()
  {
    return LockGranularity.SEGMENT;
  }

  @JsonProperty
  @Override
  public TaskLockType getType()
  {
    return type;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public Set<Integer> getPartitionIds()
  {
    return partitionIds;
  }

  @JsonProperty
  @Override
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  @Override
  public Integer getPriority()
  {
    return priority;
  }

  @Override
  public int getNonNullPriority()
  {
    return priority;
  }

  @JsonProperty
  @Override
  public boolean isRevoked()
  {
    return revoked;
  }

  @Override
  public boolean conflict(LockRequest request)
  {
    if (request instanceof SegmentLockRequest) {
      // Lock conflicts only if the interval is same and the partitionIds intersect.
      final SegmentLockRequest segmentLockRequest = (SegmentLockRequest) request;

      if (dataSource.equals(segmentLockRequest.getDataSource())
          && interval.equals(segmentLockRequest.getInterval())) {
        return !Sets.intersection(new HashSet<>(partitionIds), new HashSet<>(segmentLockRequest.getPartitionIds())).isEmpty();
      }
    }

    // For different interval, all overlapping intervals cause conflict.
    return dataSource.equals(request.getDataSource())
           && interval.overlaps(request.getInterval());
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
    SegmentLock that = (SegmentLock) o;
    return priority == that.priority &&
           revoked == that.revoked &&
           type == that.type &&
           Objects.equals(groupId, that.groupId) &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(partitionIds, that.partitionIds) &&
           Objects.equals(version, that.version);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, groupId, dataSource, interval, partitionIds, version, priority, revoked);
  }

  @Override
  public String toString()
  {
    return "SegmentLock{" +
           "type=" + type +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", partitionIds=" + partitionIds +
           ", version='" + version + '\'' +
           ", priority=" + priority +
           ", revoked=" + revoked +
           '}';
  }
}
