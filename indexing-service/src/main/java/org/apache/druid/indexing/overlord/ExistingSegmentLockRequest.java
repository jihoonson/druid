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
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.Interval;

import java.util.Set;

public class ExistingSegmentLockRequest implements LockRequest
{
  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final Set<Integer> partitionIds;
  private final String version;
  private final int priority;
  private final boolean revoked;

  public ExistingSegmentLockRequest(
      TaskLockType lockType,
      Task task,
      Interval interval,
      Set<Integer> partitionIds,
      String version
  )
  {
    this(lockType, task.getGroupId(), task.getDataSource(), interval, partitionIds, version, task.getPriority(), false);
  }

  public ExistingSegmentLockRequest(
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      Set<Integer> partitionIds,
      String version,
      int priority,
      boolean revoked
  )
  {
    this.lockType = lockType;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.partitionIds = partitionIds;
    this.version = version;
    this.priority = priority;
    this.revoked = revoked;
  }

  @Override
  public LockGranularity getGranularity()
  {
    return LockGranularity.SEGMENT;
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
  public String getVersion()
  {
    return version;
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  @Override
  public boolean isRevoked()
  {
    return revoked;
  }

  public Set<Integer> getPartitionIds()
  {
    return partitionIds;
  }

  public SegmentLock toLock()
  {
    return new SegmentLock(
        lockType,
        groupId,
        dataSource,
        interval,
        partitionIds,
        version,
        priority,
        revoked
    );
  }

  @Override
  public String toString()
  {
    return "ExistingSegmentLockRequest{" +
           "lockType=" + lockType +
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
