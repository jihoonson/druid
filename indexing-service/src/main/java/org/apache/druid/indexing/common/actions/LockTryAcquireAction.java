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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockResult;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class LockTryAcquireAction implements TaskAction<TaskLock>
{
  private final LockGranularity granularity;

  @JsonIgnore
  private final TaskLockType type;

  @JsonIgnore
  private final Interval interval;

  @Nullable
  private final String version;

  @Nullable
  private final List<Integer> partitionIds; // TODO: list -> set

  public static LockTryAcquireAction createTimeChunkRequest(TaskLockType type, Interval interval)
  {
    return new LockTryAcquireAction(LockGranularity.TIME_CHUNK, type, interval, null, null);
  }

  public static LockTryAcquireAction createSegmentRequest(
      TaskLockType type,
      Interval interval,
      String version,
      List<Integer> partitionIds
  )
  {
    Preconditions.checkNotNull(version, "version shouldn't be null for segment lock");
    Preconditions.checkState(partitionIds != null && !partitionIds.isEmpty(), "partitionIds shouldn't be empty for segment lock");

    return new LockTryAcquireAction(
        LockGranularity.SEGMENT,
        type,
        interval,
        version,
        partitionIds
    );
  }

  @JsonCreator
  private LockTryAcquireAction(
      @JsonProperty("lockGranularity") @Nullable LockGranularity granularity, // nullable for backward compatibility
      @JsonProperty("lockType") @Nullable TaskLockType type, // nullable for backward compatibility
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") @Nullable String version, // null for timeChunk lock
      @JsonProperty("partitionIds") @Nullable List<Integer> partitionIds // null for timeChunk lock
  )
  {
    this.granularity = granularity == null ? LockGranularity.TIME_CHUNK : granularity;
    this.type = type == null ? TaskLockType.EXCLUSIVE : type;
    this.interval = interval;
    this.version = version;
    this.partitionIds = partitionIds;
  }

  @JsonProperty("lockGranularity")
  public LockGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty("lockType")
  public TaskLockType getType()
  {
    return type;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public List<Integer> getPartitionIds()
  {
    return partitionIds;
  }

  @Override
  public TypeReference<TaskLock> getReturnTypeReference()
  {
    return new TypeReference<TaskLock>()
    {
    };
  }

  @Override
  public TaskLock perform(Task task, TaskActionToolbox toolbox)
  {
    final LockResult result = toolbox.getTaskLockbox().tryLock(granularity, type, task, interval, version, partitionIds);
    return result.isOk() ? result.getTaskLock() : null;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "LockTryAcquireAction{" +
           "granularity=" + granularity +
           ", type=" + type +
           ", interval=" + interval +
           ", partitionIds=" + partitionIds +
           '}';
  }
}
