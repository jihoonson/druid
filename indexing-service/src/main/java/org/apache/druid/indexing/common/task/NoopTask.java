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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 */
public class NoopTask extends AbstractTask
{
  private static final Logger log = new Logger(NoopTask.class);
  private static final int defaultRunTime = 2500;
  private static final int defaultIsReadyTime = 0;
  private static final IsReadyResult defaultIsReadyResult = IsReadyResult.YES;

  enum IsReadyResult
  {
    YES,
    NO,
    EXCEPTION
  }

  @JsonIgnore
  private final long runTime;

  @JsonIgnore
  private final long isReadyTime;

  @JsonIgnore
  private final IsReadyResult isReadyResult;

  @JsonIgnore
  private final FirehoseFactory firehoseFactory;

  @JsonCreator
  public NoopTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("runTime") long runTime,
      @JsonProperty("isReadyTime") long isReadyTime,
      @JsonProperty("isReadyResult") String isReadyResult,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? StringUtils.format("noop_%s_%s", DateTimes.nowUtc(), UUID.randomUUID().toString()) : id,
        dataSource == null ? "none" : dataSource,
        context
    );

    this.runTime = (runTime == 0) ? defaultRunTime : runTime;
    this.isReadyTime = (isReadyTime == 0) ? defaultIsReadyTime : isReadyTime;
    this.isReadyResult = (isReadyResult == null)
                         ? defaultIsReadyResult
                         : IsReadyResult.valueOf(StringUtils.toUpperCase(isReadyResult));
    this.firehoseFactory = firehoseFactory;
  }

  @Override
  public String getType()
  {
    return "noop";
  }

  @JsonProperty
  public long getRunTime()
  {
    return runTime;
  }

  @JsonProperty
  public long getIsReadyTime()
  {
    return isReadyTime;
  }

  @JsonProperty
  public IsReadyResult getIsReadyResult()
  {
    return isReadyResult;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    switch (isReadyResult) {
      case YES:
        return true;
      case NO:
        return false;
      case EXCEPTION:
        throw new ISE("Not ready. Never will be ready. Go away!");
      default:
        throw new AssertionError("#notreached");
    }
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (firehoseFactory != null) {
      log.info("Connecting firehose");
    }
    try (Firehose firehose = firehoseFactory != null ? firehoseFactory.connect(null, null) : null) {

      log.info("Running noop task[%s]", getId());
      log.info("Sleeping for %,d millis.", runTime);
      Thread.sleep(runTime);
      log.info("Woke up!");
      return TaskStatus.success(getId());
    }
  }

  @Override
  public boolean requireLockInputSegments()
  {
    return false;
  }

  @Override
  public List<DataSegment> getInputSegments(
      TaskActionClient taskActionClient, List<Interval> intervals
  )
  {
    return Collections.emptyList();
  }

  @Override
  public boolean changeSegmentGranularity(List<Interval> intervalOfExistingSegments)
  {
    return false;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity(Interval interval)
  {
    return null;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  public static NoopTask create()
  {
    return new NoopTask(null, null, 0, 0, null, null, null);
  }

  @VisibleForTesting
  public static NoopTask create(String dataSource)
  {
    return new NoopTask(null, dataSource, 0, 0, null, null, null);
  }

  @VisibleForTesting
  public static NoopTask create(int priority)
  {
    return new NoopTask(null, null, 0, 0, null, null, ImmutableMap.of(Tasks.PRIORITY_KEY, priority));
  }

  @VisibleForTesting
  public static NoopTask create(String id, int priority)
  {
    return new NoopTask(id, null, 0, 0, null, null, ImmutableMap.of(Tasks.PRIORITY_KEY, priority));
  }
}
