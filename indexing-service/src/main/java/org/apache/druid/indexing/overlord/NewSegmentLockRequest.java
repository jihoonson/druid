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

public class NewSegmentLockRequest implements LockRequest
{
  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final int priority;
  private final int numNewSegments;
  private final String baseSequenceName;
  @Nullable
  private final String previsousSegmentId;
  private final boolean skipSegmentLineageCheck;

  public NewSegmentLockRequest(
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      int priority,
      int numNewSegments,
      String baseSequenceName,
      String previsousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    this.lockType = lockType;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.priority = priority;
    this.numNewSegments = numNewSegments;
    this.baseSequenceName = baseSequenceName;
    this.previsousSegmentId = previsousSegmentId;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
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
  public int getPriority()
  {
    return priority;
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

  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  @Override
  public String toString()
  {
    return "NewSegmentLockRequest{" +
           "lockType=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", priority=" + priority +
           ", numNewSegments=" + numNewSegments +
           ", baseSequenceName='" + baseSequenceName + '\'' +
           ", previsousSegmentId='" + previsousSegmentId + '\'' +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           '}';
  }
}
