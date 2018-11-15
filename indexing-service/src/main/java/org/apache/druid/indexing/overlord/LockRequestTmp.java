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

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

public class LockRequestTmp
{
  private final LockGranularity granularity;
  private final TaskLockType type;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  @Nullable
  private final String preferredVersion;
  // partitionIds is empty for timeChunkLock and segmentLock for new segments
  private final Set<Integer> partitionIds;
  private final int priority;
  private final boolean revoked;

  public LockRequestTmp(
      LockGranularity granularity,
      TaskLockType type,
      String groupId,
      String dataSource,
      Interval interval,
      @Nullable String preferredVersion,
      @Nullable Set<Integer> partitionIds,
      int priority,
      boolean revoked
  )
  {
    this.granularity = granularity;
    this.type = type;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.preferredVersion = preferredVersion;
    this.partitionIds = partitionIds == null ? Collections.emptySet() : partitionIds;
    this.priority = priority;
    this.revoked = revoked;
  }

  public LockGranularity getGranularity()
  {
    return granularity;
  }

  public TaskLockType getType()
  {
    return type;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  public String getPreferredVersion()
  {
    return preferredVersion;
  }

  public String getVersion()
  {
    return preferredVersion == null ? DateTimes.nowUtc().toString() : preferredVersion;
  }

  public Set<Integer> getPartitionIds()
  {
    return partitionIds;
  }

  public int getPriority()
  {
    return priority;
  }

  public boolean isRevoked()
  {
    return revoked;
  }
}
