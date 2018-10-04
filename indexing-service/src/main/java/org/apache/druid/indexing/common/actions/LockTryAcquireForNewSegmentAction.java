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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.Interval;

import java.util.Collections;

public class LockTryAcquireForNewSegmentAction implements TaskAction<LockResult>
{
  private final TaskLockType type;
  private final Interval interval;
  private final String sequenceName;

  @JsonCreator
  public LockTryAcquireForNewSegmentAction(
      @JsonProperty("lockType") TaskLockType type,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("sequenceName") String sequenceName
  )
  {
    this.type = Preconditions.checkNotNull(type, "type");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
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
  public String getSequenceName()
  {
    return sequenceName;
  }

  @Override
  public TypeReference<LockResult> getReturnTypeReference()
  {
    return new TypeReference<LockResult>()
    {
    };
  }

  @Override
  public LockResult perform(Task task, TaskActionToolbox toolbox)
  {
    final String dataSource = task.getDataSource();

    // TODO: probably doInCriticalSection??
    final Pair<String, Integer> maxVersionAndPartitionId = toolbox.getIndexerMetadataStorageCoordinator().findMaxVersionAndAvailablePartitionId(
        dataSource,
        sequenceName,
        null, // previousSegmentId
        interval,
        true // skipSegmentLineageCheck
    );
    return toolbox.getTaskLockbox().tryLock(
        LockGranularity.SEGMENT,
        TaskLockType.EXCLUSIVE,
        task,
        interval,
        Collections.singletonList(maxVersionAndPartitionId.rhs)
    );
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }
}
