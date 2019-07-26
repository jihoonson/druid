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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.SubTaskCompleteEvent;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * TODO
 */
public class PartialSegmentGenerateParallelIndexTaskRunner
    implements ParallelIndexTaskRunner<PartialSegmentGenerateTask, GeneratedPartitionsReport>
{
  private static final Logger LOG = new Logger(PartialSegmentGenerateParallelIndexTaskRunner.class);

  private final TaskToolbox toolbox;
  private final String taskId;
  private final String groupId;
  private final ParallelIndexIngestionSpec ingestionSpec;
  private final Map<String, Object> context;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final int maxNumTasks;
  private final IndexingServiceClient indexingServiceClient;

  private final BlockingQueue<SubTaskCompleteEvent<ParallelIndexSubTask>> taskCompleteEvents =
      new LinkedBlockingDeque<>();

  private final ConcurrentHashMap<String, GeneratedPartitionsReport> reportsMap = new ConcurrentHashMap<>();

  private volatile boolean subTaskScheduleAndMonitorStopped;
  private volatile TaskMonitor<ParallelIndexSubTask> taskMonitor;

  private int nextSpecId = 0;

  PartialSegmentGenerateParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSpec,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.groupId = groupId;
    this.ingestionSpec = ingestionSpec;
    this.context = context;
    this.baseFirehoseFactory = (FiniteFirehoseFactory) ingestionSpec.getIOConfig().getFirehoseFactory();
    this.maxNumTasks = ingestionSpec.getTuningConfig().getMaxNumSubTasks();
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
  }

  @Override
  public TaskState run() throws Exception
  {
    if (baseFirehoseFactory.getNumSplits() == 0) {
      LOG.warn("There's no input split to process");
      return TaskState.SUCCESS;
    }


    return null;
  }

  @Override
  public void stopGracefully()
  {

  }

  @Override
  public void collectReport(GeneratedPartitionsReport report)
  {

  }

  @Override
  public ParallelIndexingProgress getProgress()
  {
    return null;
  }

  @Override
  public Set<String> getRunningTaskIds()
  {
    return null;
  }

  @Override
  public List<SubTaskSpec<PartialSegmentGenerateTask>> getSubTaskSpecs()
  {
    return null;
  }

  @Override
  public List<SubTaskSpec<PartialSegmentGenerateTask>> getRunningSubTaskSpecs()
  {
    return null;
  }

  @Override
  public List<SubTaskSpec<PartialSegmentGenerateTask>> getCompleteSubTaskSpecs()
  {
    return null;
  }

  @Nullable
  @Override
  public SubTaskSpec<PartialSegmentGenerateTask> getSubTaskSpec(String subTaskSpecId)
  {
    return null;
  }

  @Nullable
  @Override
  public SubTaskSpecStatus getSubTaskState(String subTaskSpecId)
  {
    return null;
  }

  @Nullable
  @Override
  public TaskHistory<PartialSegmentGenerateTask> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId)
  {
    return null;
  }
}
