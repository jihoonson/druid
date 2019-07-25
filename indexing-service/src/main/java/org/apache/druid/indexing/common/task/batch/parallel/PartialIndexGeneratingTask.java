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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.CachingLocalSegmentAllocator;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskSegmentAllocator;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.GeneratedPartitionsReport.PartitionStat;
import org.apache.druid.indexing.worker.ShuffleDataSegmentPusher;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PartialIndexGeneratingTask extends AbstractBatchIndexTask
{
  public static final String TYPE = "partial_index_generator";

  private static final Logger log = new Logger(PartialIndexGeneratingTask.class);

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexTaskClient> taskClientFactory;

  private FireDepartmentMetrics buildSegmentsFireDepartmentMetrics;

  @JsonCreator
  public PartialIndexGeneratingTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexTaskClient> taskClientFactory
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context
    );

    Preconditions.checkArgument(
        ingestionSchema.getTuningConfig().isForceGuaranteedRollup(),
        "forceGuaranteedRollup must be set"
    );
    Preconditions.checkArgument(
        ingestionSchema.getTuningConfig().getPartitionsSpec() instanceof HashedPartitionsSpec,
        "Please use hashed_partitions for perfect rollup"
    );
    Preconditions.checkArgument(
        !ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty(),
        "Missing intervals in granularitySpec"
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.indexingServiceClient = indexingServiceClient;
    this.taskClientFactory = taskClientFactory;
  }

  @JsonProperty
  public int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty("spec")
  public ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
  {
    throw new UnsupportedOperationException(
        "This method should be never called because it's supposed to be called only with segment locking"
        + " but ParallelIndexGeneratingTask always uses timeChunk locking."
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return true;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return tryTimeChunkLock(
        taskActionClient,
        getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals()
    );
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
    // Firehose temporary directory is automatically removed when this IndexTask completes.
    FileUtils.forceMkdir(firehoseTempDir);

    final ParallelIndexTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );

    final List<DataSegment> segments = generateSegments(toolbox, firehoseFactory, firehoseTempDir);
    final List<PartitionStat> partitionStats = segments
        .stream()
        .map(segment -> new PartitionStat(segment.getShardSpec().getPartitionNum(), null, null))
        .collect(Collectors.toList());
    taskClient.report(supervisorTaskId, new GeneratedPartitionsReport(getId(), partitionStats));

    return TaskStatus.success(getId());
  }

  private List<DataSegment> generateSegments(
      final TaskToolbox toolbox,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema,
        new RealtimeIOConfig(null, null),
        null
    );
    buildSegmentsFireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              Collections.singletonList(fireDepartmentForMetrics),
              Collections.singletonMap(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final HashedPartitionsSpec partitionsSpec = (HashedPartitionsSpec) tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final Map<Interval, Pair<ShardSpecFactory, Integer>> shardSpecs = IndexTask.createShardSpecWithoutInputScan(
        granularitySpec,
        ingestionSchema.getIOConfig(),
        tuningConfig,
        partitionsSpec
    );

    final IndexTaskSegmentAllocator segmentAllocator = new CachingLocalSegmentAllocator(
        toolbox,
        getId(),
        getDataSource(),
        shardSpecs
    );

    try (
        final Appenderator appenderator = IndexTask.newAppenderator(
            buildSegmentsFireDepartmentMetrics,
            toolbox,
            dataSchema,
            tuningConfig,
            new ShuffleDataSegmentPusher(supervisorTaskId, getId(), toolbox.getIntermediaryDataManager())
        );
        final BatchAppenderatorDriver driver = IndexTask.newDriver(appenderator, toolbox, segmentAllocator);
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      driver.startJob();

      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            buildSegmentsMeters.incrementThrownAway();
            continue;
          }

          if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                "Encountered row with timestamp that cannot be represented as a long: [%s]",
                inputRow
            );
            throw new ParseException(errorMsg);
          }

          final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          if (!optInterval.isPresent()) {
            buildSegmentsMeters.incrementThrownAway();
            continue;
          }

          final Interval interval = optInterval.get();
          final String sequenceName = segmentAllocator.getSequenceName(interval, inputRow);
          final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName);

          if (addResult.isOk()) {

            // incremental segment publishment is allowed only when rollup don't have to be perfect.
            if (dynamicPartitionsSpec != null) {
              final boolean isPushRequired = addResult.isPushRequired(
                  dynamicPartitionsSpec.getMaxRowsPerSegment(),
                  dynamicPartitionsSpec.getMaxTotalRows()
              );
              if (isPushRequired) {
                // There can be some segments waiting for being published even though any rows won't be added to them.
                // If those segments are not published here, the available space in appenderator will be kept to be
                // small which makes the size of segments smaller.
                final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
                log.info("Pushed segments[%s]", pushed.getSegments());
              }
            }
          } else {
            throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
          }

          if (addResult.getParseException() != null) {
            handleParseException(addResult.getParseException());
          } else {
            buildSegmentsMeters.incrementProcessed();
          }
        }
        catch (ParseException e) {
          handleParseException(e);
        }
      }

      final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
      log.info("Pushed segments[%s]", pushed.getSegments());

      return pushed.getSegments();
    }
  }
}
