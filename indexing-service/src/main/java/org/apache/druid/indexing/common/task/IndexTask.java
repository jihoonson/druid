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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IOConfig;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.apache.druid.utils.CircularBuffer;
import org.codehaus.plexus.util.FileUtils;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IndexTask extends AbstractBatchIndexTask implements ChatHandler
{
  private static final Logger log = new Logger(IndexTask.class);
  private static final HashFunction hashFunction = Hashing.murmur3_128();
  private static final String TYPE = "index";

  private static String makeGroupId(IndexIngestionSpec ingestionSchema)
  {
    return makeGroupId(ingestionSchema.ioConfig.appendToExisting, ingestionSchema.dataSchema.getDataSource());
  }

  private static String makeGroupId(boolean isAppendToExisting, String dataSource)
  {
    if (isAppendToExisting) {
      // Shared locking group for all tasks that append, since they are OK to run concurrently.
      return StringUtils.format("%s_append_%s", TYPE, dataSource);
    } else {
      // Return null, one locking group per task.
      return null;
    }
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  @JsonIgnore
  private IngestionState ingestionState;

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final Optional<ChatHandlerProvider> chatHandlerProvider;

  @JsonIgnore
  private final RowIngestionMeters determinePartitionsMeters;

  @JsonIgnore
  private final RowIngestionMeters buildSegmentsMeters;

  @JsonIgnore
  private FireDepartmentMetrics buildSegmentsFireDepartmentMetrics;

  @JsonIgnore
  private CircularBuffer<Throwable> buildSegmentsSavedParseExceptions;

  @JsonIgnore
  private CircularBuffer<Throwable> determinePartitionsSavedParseExceptions;

  @JsonIgnore
  private String errorMsg;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("spec") final IndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this(
        id,
        makeGroupId(ingestionSchema),
        taskResource,
        ingestionSchema.dataSchema.getDataSource(),
        ingestionSchema,
        context,
        authorizerMapper,
        chatHandlerProvider,
        rowIngestionMetersFactory
    );
  }

  public IndexTask(
      String id,
      String groupId,
      TaskResource resource,
      String dataSource,
      IndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      AuthorizerMapper authorizerMapper,
      ChatHandlerProvider chatHandlerProvider,
      RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSource),
        groupId,
        resource,
        dataSource,
        context
    );
    this.ingestionSchema = ingestionSchema;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    if (ingestionSchema.getTuningConfig().getMaxSavedParseExceptions() > 0) {
      determinePartitionsSavedParseExceptions = new CircularBuffer<>(
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );

      buildSegmentsSavedParseExceptions = new CircularBuffer<>(
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );
    }
    this.ingestionState = IngestionState.NOT_STARTED;
    this.determinePartitionsMeters = rowIngestionMetersFactory.createRowIngestionMeters();
    this.buildSegmentsMeters = rowIngestionMetersFactory.createRowIngestionMeters();
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
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return determineLockGranularityAndTryLock(taskActionClient, ingestionSchema.dataSchema.getGranularitySpec());
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return isGuaranteedRollup(ingestionSchema.ioConfig, ingestionSchema.tuningConfig)
           || !ingestionSchema.ioConfig.isAppendToExisting();
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return findInputSegments(
        getDataSource(),
        taskActionClient,
        intervals,
        ingestionSchema.ioConfig.firehoseFactory
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return isGuaranteedRollup(ingestionSchema.ioConfig, ingestionSchema.tuningConfig);
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

  @GET
  @Path("/unparseableEvents")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUnparseableEvents(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    Map<String, List<String>> events = new HashMap<>();

    boolean needsDeterminePartitions = false;
    boolean needsBuildSegments = false;

    if (full != null) {
      needsDeterminePartitions = true;
      needsBuildSegments = true;
    } else {
      switch (ingestionState) {
        case DETERMINE_PARTITIONS:
          needsDeterminePartitions = true;
          break;
        case BUILD_SEGMENTS:
        case COMPLETED:
          needsBuildSegments = true;
          break;
        default:
          break;
      }
    }

    if (needsDeterminePartitions) {
      events.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          IndexTaskUtils.getMessagesFromSavedParseExceptions(determinePartitionsSavedParseExceptions)
      );
    }

    if (needsBuildSegments) {
      events.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          IndexTaskUtils.getMessagesFromSavedParseExceptions(buildSegmentsSavedParseExceptions)
      );
    }

    return Response.ok(events).build();
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();
    Map<String, Object> averagesMap = new HashMap<>();

    boolean needsDeterminePartitions = false;
    boolean needsBuildSegments = false;

    if (full != null) {
      needsDeterminePartitions = true;
      needsBuildSegments = true;
    } else {
      switch (ingestionState) {
        case DETERMINE_PARTITIONS:
          needsDeterminePartitions = true;
          break;
        case BUILD_SEGMENTS:
        case COMPLETED:
          needsBuildSegments = true;
          break;
        default:
          break;
      }
    }

    if (needsDeterminePartitions) {
      totalsMap.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          determinePartitionsMeters.getTotals()
      );
      averagesMap.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          determinePartitionsMeters.getMovingAverages()
      );
    }

    if (needsBuildSegments) {
      totalsMap.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          buildSegmentsMeters.getTotals()
      );
      averagesMap.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          buildSegmentsMeters.getMovingAverages()
      );
    }

    returnMap.put("totals", totalsMap);
    returnMap.put("movingAverages", averagesMap);
    return Response.ok(returnMap).build();
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox)
  {
    try {
      if (chatHandlerProvider.isPresent()) {
        log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());

        if (chatHandlerProvider.get().get(getId()).isPresent()) {
          // This is a workaround for ParallelIndexSupervisorTask to avoid double registering when it runs in the
          // sequential mode. See ParallelIndexSupervisorTask.runSequential().
          // Note that all HTTP endpoints are not available in this case. This works only for
          // ParallelIndexSupervisorTask because it doesn't support APIs for live ingestion reports.
          log.warn("Chat handler is already registered. Skipping chat handler registration.");
        } else {
          chatHandlerProvider.get().register(getId(), this, false);
        }
      } else {
        log.warn("No chat handler detected");
      }

      final boolean determineIntervals = !ingestionSchema.getDataSchema()
                                                         .getGranularitySpec()
                                                         .bucketIntervals()
                                                         .isPresent();

      final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

      final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
      // Firehose temporary directory is automatically removed when this IndexTask completes.
      FileUtils.forceMkdir(firehoseTempDir);

      ingestionState = IngestionState.DETERMINE_PARTITIONS;

      // Initialize maxRowsPerSegment and maxTotalRows lazily
      final IndexTuningConfig tuningConfig = ingestionSchema.tuningConfig;
      @Nullable
      final Integer maxRowsPerSegment = getValidMaxRowsPerSegment(tuningConfig);
      @Nullable
      final Long maxTotalRows = getValidMaxTotalRows(tuningConfig);
      // Spec for segment allocation. This is used only for perfect rollup mode.
      // See createSegmentAllocator().
      final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec = determineShardSpecs(
          toolbox,
          firehoseFactory,
          firehoseTempDir,
          maxRowsPerSegment
      );

      final List<Interval> allocateIntervals = new ArrayList<>(allocateSpec.keySet());
      final DataSchema dataSchema;
      if (determineIntervals) {
        if (!determineLockGranularityandTryLock(toolbox.getTaskActionClient(), allocateIntervals)) {
          throw new ISE("Failed to get locks for intervals[%s]", allocateIntervals);
        }

        dataSchema = ingestionSchema.getDataSchema().withGranularitySpec(
            ingestionSchema.getDataSchema()
                           .getGranularitySpec()
                           .withIntervals(JodaUtils.condenseIntervals(allocateIntervals))
        );
      } else {
        dataSchema = ingestionSchema.getDataSchema();
      }

      ingestionState = IngestionState.BUILD_SEGMENTS;
      return generateAndPublishSegments(
          toolbox,
          dataSchema,
          allocateSpec,
          firehoseFactory,
          firehoseTempDir,
          maxRowsPerSegment,
          maxTotalRows
      );
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in %s.", ingestionState);
      errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
      return TaskStatus.failure(
          getId(),
          errorMsg
      );
    }

    finally {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
    }
  }

  private Map<String, TaskReport> getTaskCompletionReports()
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg
            )
        )
    );
  }

  private Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<String> determinePartitionsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        determinePartitionsSavedParseExceptions);
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        buildSegmentsSavedParseExceptions);

    if (determinePartitionsParseExceptionMessages != null || buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.DETERMINE_PARTITIONS, determinePartitionsParseExceptionMessages);
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }

    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        determinePartitionsMeters.getTotals()
    );

    metrics.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        buildSegmentsMeters.getTotals()
    );

    return metrics;
  }

  private static boolean isGuaranteedRollup(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    Preconditions.checkState(
        !tuningConfig.isForceGuaranteedRollup() || !ioConfig.isAppendToExisting(),
        "Perfect rollup cannot be guaranteed when appending to existing dataSources"
    );
    return tuningConfig.isForceGuaranteedRollup();
  }

  /**
   * Determines intervals and shardSpecs for input data.  This method first checks that it must determine intervals and
   * shardSpecs by itself.  Intervals must be determined if they are not specified in {@link GranularitySpec}.
   * ShardSpecs must be determined if the perfect rollup must be guaranteed even though the number of shards is not
   * specified in {@link IndexTuningConfig}.
   * <p/>
   * If both intervals and shardSpecs don't have to be determined, this method simply returns {@link ShardSpecs} for the
   * given intervals.  Here, if {@link IndexTuningConfig#numShards} is not specified, {@link NumberedShardSpec} is used.
   * <p/>
   * If one of intervals or shardSpecs need to be determined, this method reads the entire input for determining one of
   * them.  If the perfect rollup must be guaranteed, {@link HashBasedNumberedShardSpec} is used for hash partitioning
   * of input data.  In the future we may want to also support single-dimension partitioning.
   *
   * @return generated {@link ShardSpecs} representing a map of intervals and corresponding shard specs
   */
  private Map<Interval, Pair<ShardSpecFactory, Integer>> determineShardSpecs(
      final TaskToolbox toolbox,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir,
      @Nullable final Integer maxRowsPerSegment
  ) throws IOException
  {
    final ObjectMapper jsonMapper = toolbox.getObjectMapper();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();

    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    // Must determine intervals if unknown, since we acquire all locks before processing any data.
    final boolean determineIntervals = !granularitySpec.bucketIntervals().isPresent();

    // Must determine partitions if rollup is guaranteed and the user didn't provide a specific value.
    final boolean determineNumPartitions = tuningConfig.getNumShards() == null
                                           && isGuaranteedRollup(ioConfig, tuningConfig);

    // if we were given number of shards per interval and the intervals, we don't need to scan the data
    if (!determineNumPartitions && !determineIntervals) {
      log.info("Skipping determine partition scan");
      return createShardSpecWithoutInputScan(granularitySpec, ioConfig, tuningConfig);
    } else {
      // determine intervals containing data and prime HLL collectors
      return createShardSpecsFromInput(
          jsonMapper,
          ingestionSchema,
          firehoseFactory,
          firehoseTempDir,
          granularitySpec,
          tuningConfig,
          determineIntervals,
          determineNumPartitions,
          maxRowsPerSegment
      );
    }
  }

  private Map<Interval, Pair<ShardSpecFactory, Integer>> createShardSpecWithoutInputScan(
      GranularitySpec granularitySpec,
      IndexIOConfig ioConfig,
      IndexTuningConfig tuningConfig
  )
  {
    final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec = new HashMap<>();
    final SortedSet<Interval> intervals = granularitySpec.bucketIntervals().get();

    if (isGuaranteedRollup(ioConfig, tuningConfig)) {
      // Overwrite mode, guaranteed rollup: shardSpecs must be known in advance.
      final int numShards = tuningConfig.getNumShards() == null ? 1 : tuningConfig.getNumShards();
      for (Interval interval : intervals) {
        allocateSpec.put(
            interval,
            createShardSpecFactoryForGuaranteedRollup(numShards, tuningConfig.partitionDimensions)
        );
      }
    } else {
      for (Interval interval : intervals) {
        allocateSpec.put(interval, null);
      }
    }

    return allocateSpec;
  }

  private Map<Interval, Pair<ShardSpecFactory, Integer>> createShardSpecsFromInput(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean determineIntervals,
      boolean determineNumPartitions,
      @Nullable Integer maxRowsPerSegment
  ) throws IOException
  {
    log.info("Determining intervals and shardSpecs");
    long determineShardSpecsStartMillis = System.currentTimeMillis();

    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = collectIntervalsAndShardSpecs(
        jsonMapper,
        ingestionSchema,
        firehoseFactory,
        firehoseTempDir,
        granularitySpec,
        determineIntervals,
        determineNumPartitions
    );

    final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpecs = new HashMap<>();
    final int defaultNumShards = tuningConfig.getNumShards() == null ? 1 : tuningConfig.getNumShards();
    for (final Map.Entry<Interval, Optional<HyperLogLogCollector>> entry : hllCollectors.entrySet()) {
      final Interval interval = entry.getKey();
      final HyperLogLogCollector collector = entry.getValue().orNull();

      final int numShards;
      if (determineNumPartitions) {
        final long numRows = Preconditions.checkNotNull(collector, "HLL collector").estimateCardinalityRound();
        numShards = (int) Math.ceil(
            (double) numRows / Preconditions.checkNotNull(maxRowsPerSegment, "maxRowsPerSegment")
        );
        log.info("Estimated [%,d] rows of data for interval [%s], creating [%,d] shards", numRows, interval, numShards);
      } else {
        numShards = defaultNumShards;
        log.info("Creating [%,d] shards for interval [%s]", numShards, interval);
      }

      if (isGuaranteedRollup(ingestionSchema.getIOConfig(), ingestionSchema.getTuningConfig())) {
        // Overwrite mode, guaranteed rollup: # of shards must be known in advance.
        allocateSpecs.put(
            interval,
            createShardSpecFactoryForGuaranteedRollup(numShards, tuningConfig.partitionDimensions)
        );
      } else {
        allocateSpecs.put(interval, null);
      }
    }
    log.info("Found intervals and shardSpecs in %,dms", System.currentTimeMillis() - determineShardSpecsStartMillis);

    return allocateSpecs;
  }

  private Pair<ShardSpecFactory, Integer> createShardSpecFactoryForGuaranteedRollup(
      int numShards,
      @Nullable List<String> partitionDimensions
  )
  {
    return Pair.of(new HashBasedNumberedShardSpecFactory(partitionDimensions, numShards), numShards);
  }

  private Map<Interval, Optional<HyperLogLogCollector>> collectIntervalsAndShardSpecs(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      boolean determineIntervals,
      boolean determineNumPartitions
  ) throws IOException
  {
    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = new TreeMap<>(
        Comparators.intervalsByStartThenEnd()
    );
    final Granularity queryGranularity = granularitySpec.getQueryGranularity();

    try (
        final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser(), firehoseTempDir)
    ) {

      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          // The null inputRow means the caller must skip this row.
          if (inputRow == null) {
            determinePartitionsMeters.incrementThrownAway();
            continue;
          }

          final Interval interval;
          if (determineIntervals) {
            interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
          } else {
            if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
              final String errorMsg = StringUtils.format(
                  "Encountered row with timestamp that cannot be represented as a long: [%s]",
                  inputRow
              );
              throw new ParseException(errorMsg);
            }

            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              determinePartitionsMeters.incrementThrownAway();
              continue;
            }
            interval = optInterval.get();
          }

          if (determineNumPartitions) {
            hllCollectors.computeIfAbsent(interval, intv -> Optional.of(HyperLogLogCollector.makeLatestCollector()));

            List<Object> groupKey = Rows.toGroupKey(
                queryGranularity.bucketStart(inputRow.getTimestamp()).getMillis(),
                inputRow
            );
            hllCollectors.get(interval).get()
                         .add(hashFunction.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asBytes());
          } else {
            // we don't need to determine partitions but we still need to determine intervals, so add an Optional.absent()
            // for the interval and don't instantiate a HLL collector
            hllCollectors.putIfAbsent(interval, Optional.absent());
          }
          determinePartitionsMeters.incrementProcessed();
        }
        catch (ParseException e) {
          if (ingestionSchema.getTuningConfig().isLogParseExceptions()) {
            log.error(e, "Encountered parse exception: ");
          }

          if (determinePartitionsSavedParseExceptions != null) {
            determinePartitionsSavedParseExceptions.add(e);
          }

          determinePartitionsMeters.incrementUnparseable();
          if (determinePartitionsMeters.getUnparseable() > ingestionSchema.getTuningConfig().getMaxParseExceptions()) {
            throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
          }
        }
      }
    }

    // These metrics are reported in generateAndPublishSegments()
    if (determinePartitionsMeters.getThrownAway() > 0) {
      log.warn("Unable to find a matching interval for [%,d] events", determinePartitionsMeters.getThrownAway());
    }
    if (determinePartitionsMeters.getUnparseable() > 0) {
      log.warn("Unable to parse [%,d] events", determinePartitionsMeters.getUnparseable());
    }

    return hllCollectors;
  }

  private IndexTaskSegmentAllocator createSegmentAllocator(
      TaskToolbox toolbox,
      DataSchema dataSchema,
      Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec
  ) throws IOException
  {
    if (ingestionSchema.ioConfig.isAppendToExisting() || isUseSegmentLock()) {
      return new RemoteSegmentAllocator(
          toolbox,
          getId(),
          dataSchema,
          getNonNullSegmentLockHelper(),
          isUseSegmentLock() ? LockGranularity.SEGMENT : LockGranularity.TIME_CHUNK,
          ingestionSchema.ioConfig.isAppendToExisting()
      );
    } else {
      // We use the timeChunk lock and don't have to ask the overlord to create segmentIds.
      // Instead, a local allocator is used.
      if (isGuaranteedRollup(ingestionSchema.ioConfig, ingestionSchema.tuningConfig)) {
        return new CachingLocalSegmentAllocator(toolbox, getId(), getDataSource(), allocateSpec);
      } else {
        return new LocalSegmentAllocator(toolbox, getId(), getDataSource(), dataSchema.getGranularitySpec());
      }
    }
  }

  /**
   * This method reads input data row by row and adds the read row to a proper segment using {@link BaseAppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link IndexTuningConfig#maxRowsPerSegment}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link IndexTuningConfig#maxTotalRows}
   * </li>
   * </ul>
   * <p>
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private TaskStatus generateAndPublishSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir,
      @Nullable final Integer maxRowsPerSegment,
      @Nullable final Long maxTotalRows
  ) throws IOException, InterruptedException
  {
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null, null), null);
    buildSegmentsFireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      final TaskRealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(
          this,
          fireDepartmentForMetrics,
          buildSegmentsMeters
      );
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);
    }

    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final long pushTimeout = tuningConfig.getPushTimeout();
    final boolean isGuaranteedRollup = isGuaranteedRollup(ioConfig, tuningConfig);

    final IndexTaskSegmentAllocator segmentAllocator = createSegmentAllocator(
        toolbox,
        dataSchema,
        allocateSpec
    );

    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient()
               .submit(SegmentTransactionalInsertAction.overwriteAction(segmentsToBeOverwritten, segmentsToPublish));

    try (
        final Appenderator appenderator = newAppenderator(
            buildSegmentsFireDepartmentMetrics,
            toolbox,
            dataSchema,
            tuningConfig
        );
        final BatchAppenderatorDriver driver = newDriver(appenderator, toolbox, segmentAllocator);
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
            if (!isGuaranteedRollup && addResult.isPushRequired(maxRowsPerSegment, maxTotalRows)) {
              // There can be some segments waiting for being published even though any rows won't be added to them.
              // If those segments are not published here, the available space in appenderator will be kept to be small
              // which makes the size of segments smaller.
              final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
              log.info("Pushed segments[%s]", pushed.getSegments());
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

      // Probably we can publish atomicUpdateGroup along with segments.
      final Set<DataSegment> inputSegments = isUseSegmentLock()
                                             ? getNonNullSegmentLockHelper().getLockedExistingSegments()
                                             : null;
      final SegmentsAndMetadata published = awaitPublish(driver.publishAll(inputSegments, publisher), pushTimeout);

      ingestionState = IngestionState.COMPLETED;
      if (published == null) {
        log.error("Failed to publish segments, aborting!");
        errorMsg = "Failed to publish segments.";
        toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
        return TaskStatus.failure(
            getId(),
            errorMsg
        );
      } else {
        log.info(
            "Processed[%,d] events, unparseable[%,d], thrownAway[%,d].",
            buildSegmentsMeters.getProcessed(),
            buildSegmentsMeters.getUnparseable(),
            buildSegmentsMeters.getThrownAway()
        );
        log.info("Published segments: %s", Lists.transform(published.getSegments(), DataSegment::getId));

        toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
        return TaskStatus.success(getId());
      }
    }
    catch (TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the valid target partition size. If {@link IndexTuningConfig#numShards} is valid, this returns null.
   * Otherwise, this returns {@link IndexTuningConfig#DEFAULT_MAX_ROWS_PER_SEGMENT} or the given
   * {@link IndexTuningConfig#maxRowsPerSegment}.
   */
  public static Integer getValidMaxRowsPerSegment(IndexTuningConfig tuningConfig)
  {
    @Nullable
    final Integer numShards = tuningConfig.numShards;
    @Nullable
    final Integer maxRowsPerSegment = tuningConfig.maxRowsPerSegment;
    if (numShards == null || numShards == -1) {
      return maxRowsPerSegment == null || maxRowsPerSegment.equals(-1)
             ? IndexTuningConfig.DEFAULT_MAX_ROWS_PER_SEGMENT
             : maxRowsPerSegment;
    } else {
      return null;
    }
  }

  /**
   * Return the valid target partition size. If {@link IndexTuningConfig#numShards} is valid, this returns null.
   * Otherwise, this returns {@link IndexTuningConfig#DEFAULT_MAX_TOTAL_ROWS} or the given
   * {@link IndexTuningConfig#maxTotalRows}.
   */
  public static Long getValidMaxTotalRows(IndexTuningConfig tuningConfig)
  {
    @Nullable
    final Integer numShards = tuningConfig.numShards;
    @Nullable
    final Long maxTotalRows = tuningConfig.maxTotalRows;
    if (numShards == null || numShards == -1) {
      return maxTotalRows == null ? IndexTuningConfig.DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
    } else {
      return null;
    }
  }

  private void handleParseException(ParseException e)
  {
    if (e.isFromPartiallyValidRow()) {
      buildSegmentsMeters.incrementProcessedWithError();
    } else {
      buildSegmentsMeters.incrementUnparseable();
    }

    if (ingestionSchema.tuningConfig.isLogParseExceptions()) {
      log.error(e, "Encountered parse exception:");
    }

    if (buildSegmentsSavedParseExceptions != null) {
      buildSegmentsSavedParseExceptions.add(e);
    }

    if (buildSegmentsMeters.getUnparseable()
        + buildSegmentsMeters.getProcessedWithError() > ingestionSchema.tuningConfig.getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...", e);
    }
  }

  private static SegmentsAndMetadata awaitPublish(
      ListenableFuture<SegmentsAndMetadata> publishFuture,
      long publishTimeout
  ) throws ExecutionException, InterruptedException, TimeoutException
  {
    if (publishTimeout == 0) {
      return publishFuture.get();
    } else {
      return publishFuture.get(publishTimeout, TimeUnit.MILLISECONDS);
    }
  }

  private static Appenderator newAppenderator(
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      IndexTuningConfig tuningConfig
  )
  {
    return Appenderators.createOffline(
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9()
    );
  }

  private static BatchAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator
  )
  {
    return new BatchAppenderatorDriver(
        appenderator,
        segmentAllocator,
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller()
    );
  }

  /**
   * This class represents a map of (Interval, ShardSpec) and is used for easy shardSpec generation.
   */
  static class ShardSpecs
  {
    private final Map<Interval, List<ShardSpec>> map;

    ShardSpecs(final Map<Interval, List<ShardSpec>> map)
    {
      this.map = map;
    }

    /**
     * Return the underlying map.
     *
     * @return a map of intervals to shardSpecs
     */
    Map<Interval, List<ShardSpec>> getMap()
    {
      return map;
    }

    Set<Interval> getIntervals()
    {
      return map.keySet();
    }

    /**
     * Return a shardSpec for the given interval and input row.
     *
     * @param interval interval for shardSpec
     * @param row      input row
     *
     * @return a shardSpec
     */
    ShardSpec getShardSpec(Interval interval, InputRow row)
    {
      final List<ShardSpec> shardSpecs = map.get(interval);
      if (shardSpecs == null || shardSpecs.isEmpty()) {
        throw new ISE("Failed to get shardSpec for interval[%s]", interval);
      }
      return shardSpecs.get(0).getLookup(shardSpecs).getShardSpec(row.getTimestampFromEpoch(), row);
    }
  }

  public static class IndexIngestionSpec extends IngestionSpec<IndexIOConfig, IndexTuningConfig>
  {
    private final DataSchema dataSchema;
    private final IndexIOConfig ioConfig;
    private final IndexTuningConfig tuningConfig;

    @JsonCreator
    public IndexIngestionSpec(
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("ioConfig") IndexIOConfig ioConfig,
        @JsonProperty("tuningConfig") IndexTuningConfig tuningConfig
    )
    {
      super(dataSchema, ioConfig, tuningConfig);

      this.dataSchema = dataSchema;
      this.ioConfig = ioConfig;
      this.tuningConfig = tuningConfig == null ? new IndexTuningConfig() : tuningConfig;
    }

    @Override
    @JsonProperty("dataSchema")
    public DataSchema getDataSchema()
    {
      return dataSchema;
    }

    @Override
    @JsonProperty("ioConfig")
    public IndexIOConfig getIOConfig()
    {
      return ioConfig;
    }

    @Override
    @JsonProperty("tuningConfig")
    public IndexTuningConfig getTuningConfig()
    {
      return tuningConfig;
    }
  }

  @JsonTypeName("index")
  public static class IndexIOConfig implements IOConfig
  {
    private static final boolean DEFAULT_APPEND_TO_EXISTING = false;

    private final FirehoseFactory firehoseFactory;
    private final boolean appendToExisting;

    @JsonCreator
    public IndexIOConfig(
        @JsonProperty("firehose") FirehoseFactory firehoseFactory,
        @JsonProperty("appendToExisting") @Nullable Boolean appendToExisting
    )
    {
      this.firehoseFactory = firehoseFactory;
      this.appendToExisting = appendToExisting == null ? DEFAULT_APPEND_TO_EXISTING : appendToExisting;
    }

    @JsonProperty("firehose")
    public FirehoseFactory getFirehoseFactory()
    {
      return firehoseFactory;
    }

    @JsonProperty("appendToExisting")
    public boolean isAppendToExisting()
    {
      return appendToExisting;
    }
  }

  @JsonTypeName("index")
  public static class IndexTuningConfig implements TuningConfig, AppenderatorConfig
  {
    static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
    static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;

    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
    private static final boolean DEFAULT_GUARANTEE_ROLLUP = false;
    private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
    private static final long DEFAULT_PUSH_TIMEOUT = 0;

    @Nullable
    private final Integer maxRowsPerSegment;
    private final int maxRowsInMemory;
    private final long maxBytesInMemory;
    @Nullable
    private final Long maxTotalRows;
    @Nullable
    private final Integer numShards;
    private final List<String> partitionDimensions;
    private final IndexSpec indexSpec;
    private final File basePersistDirectory;
    private final int maxPendingPersists;

    /**
     * This flag is to force _perfect rollup mode_. {@link IndexTask} will scan the whole input data twice to 1) figure
     * out proper shard specs for each segment and 2) generate segments. Note that perfect rollup mode basically assumes
     * that no more data will be appended in the future. As a result, in perfect rollup mode,
     * {@link HashBasedNumberedShardSpec} is used for shards.
     */
    private final boolean forceGuaranteedRollup;
    private final boolean reportParseExceptions;
    private final long pushTimeout;
    private final boolean logParseExceptions;
    private final int maxParseExceptions;
    private final int maxSavedParseExceptions;

    @Nullable
    private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

    public static IndexTuningConfig createDefault()
    {
      return new IndexTuningConfig();
    }

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") @Deprecated @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
        @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
        @JsonProperty("rowFlushBoundary") @Deprecated @Nullable Integer rowFlushBoundary_forBackCompatibility,
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
        @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
        @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
        @JsonProperty("reportParseExceptions") @Deprecated @Nullable Boolean reportParseExceptions,
        @JsonProperty("publishTimeout") @Deprecated @Nullable Long publishTimeout,
        @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
        @JsonProperty("segmentWriteOutMediumFactory") @Nullable
            SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
        @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
        @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
    )
    {
      this(
          maxRowsPerSegment == null ? targetPartitionSize : maxRowsPerSegment,
          maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
          maxBytesInMemory != null ? maxBytesInMemory : 0,
          maxTotalRows,
          numShards,
          partitionDimensions,
          indexSpec,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout != null ? pushTimeout : publishTimeout,
          null,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );

      Preconditions.checkArgument(
          targetPartitionSize == null || maxRowsPerSegment == null,
          "Can't use targetPartitionSize and maxRowsPerSegment together"
      );
    }

    private IndexTuningConfig()
    {
      this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private IndexTuningConfig(
        @Nullable Integer maxRowsPerSegment,
        @Nullable Integer maxRowsInMemory,
        @Nullable Long maxBytesInMemory,
        @Nullable Long maxTotalRows,
        @Nullable Integer numShards,
        @Nullable List<String> partitionDimensions,
        @Nullable IndexSpec indexSpec,
        @Nullable Integer maxPendingPersists,
        @Nullable Boolean forceGuaranteedRollup,
        @Nullable Boolean reportParseExceptions,
        @Nullable Long pushTimeout,
        @Nullable File basePersistDirectory,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @Nullable Boolean logParseExceptions,
        @Nullable Integer maxParseExceptions,
        @Nullable Integer maxSavedParseExceptions
    )
    {
      Preconditions.checkArgument(
          maxRowsPerSegment == null || maxRowsPerSegment.equals(-1) || numShards == null || numShards.equals(-1),
          "maxRowsPerSegment and numShards cannot both be set"
      );

      this.maxRowsPerSegment = (maxRowsPerSegment != null && maxRowsPerSegment == -1)
                               ? null
                               : maxRowsPerSegment;
      this.maxRowsInMemory = maxRowsInMemory == null ? TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
      // initializing this to 0, it will be lazily initialized to a value
      // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
      this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
      this.maxTotalRows = maxTotalRows;
      this.numShards = numShards == null || numShards.equals(-1) ? null : numShards;
      this.partitionDimensions = partitionDimensions == null ? Collections.emptyList() : partitionDimensions;
      this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
      this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
      this.forceGuaranteedRollup = forceGuaranteedRollup == null ? DEFAULT_GUARANTEE_ROLLUP : forceGuaranteedRollup;
      this.reportParseExceptions = reportParseExceptions == null
                                   ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                   : reportParseExceptions;
      this.pushTimeout = pushTimeout == null ? DEFAULT_PUSH_TIMEOUT : pushTimeout;
      this.basePersistDirectory = basePersistDirectory;

      this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;

      if (this.reportParseExceptions) {
        this.maxParseExceptions = 0;
        this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
      } else {
        this.maxParseExceptions = maxParseExceptions == null
                                  ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS
                                  : maxParseExceptions;
        this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                       ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                       : maxSavedParseExceptions;
      }
      this.logParseExceptions = logParseExceptions == null
                                ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS
                                : logParseExceptions;
    }

    public IndexTuningConfig withBasePersistDirectory(File dir)
    {
      return new IndexTuningConfig(
          maxRowsPerSegment,
          maxRowsInMemory,
          maxBytesInMemory,
          maxTotalRows,
          numShards,
          partitionDimensions,
          indexSpec,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          dir,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    public IndexTuningConfig withMaxTotalRows(Long maxTotalRows)
    {
      return new IndexTuningConfig(
          maxRowsPerSegment,
          maxRowsInMemory,
          maxBytesInMemory,
          maxTotalRows,
          numShards,
          partitionDimensions,
          indexSpec,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          basePersistDirectory,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    public IndexTuningConfig withMaxRowsPerSegment(int maxRowsPerSegment)
    {
      return new IndexTuningConfig(
          maxRowsPerSegment,
          maxRowsInMemory,
          maxBytesInMemory,
          maxTotalRows,
          numShards,
          partitionDimensions,
          indexSpec,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          basePersistDirectory,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    /**
     * Return the max number of rows per segment. This returns null if it's not specified in tuningConfig.
     * Please use {@link IndexTask#getValidMaxRowsPerSegment} instead to get the valid value.
     */
    @Nullable
    @JsonProperty
    @Override
    public Integer getMaxRowsPerSegment()
    {
      return maxRowsPerSegment;
    }

    @JsonProperty
    @Override
    public int getMaxRowsInMemory()
    {
      return maxRowsInMemory;
    }

    @JsonProperty
    @Override
    public long getMaxBytesInMemory()
    {
      return maxBytesInMemory;
    }

    /**
     * Return the max number of total rows in appenderator. This returns null if it's not specified in tuningConfig.
     * Please use {@link IndexTask#getValidMaxTotalRows} instead to get the valid value.
     */
    @JsonProperty
    @Override
    @Nullable
    public Long getMaxTotalRows()
    {
      return maxTotalRows;
    }

    @JsonProperty
    public Integer getNumShards()
    {
      return numShards;
    }

    @JsonProperty
    public List<String> getPartitionDimensions()
    {
      return partitionDimensions;
    }

    @JsonProperty
    @Override
    public IndexSpec getIndexSpec()
    {
      return indexSpec;
    }

    @Override
    public File getBasePersistDirectory()
    {
      return basePersistDirectory;
    }

    @JsonProperty
    @Override
    public int getMaxPendingPersists()
    {
      return maxPendingPersists;
    }

    /**
     * Always returns true, doesn't affect the version being built.
     */
    @Deprecated
    @JsonProperty
    public boolean isBuildV9Directly()
    {
      return true;
    }

    @JsonProperty
    public boolean isForceGuaranteedRollup()
    {
      return forceGuaranteedRollup;
    }

    @JsonProperty
    @Override
    public boolean isReportParseExceptions()
    {
      return reportParseExceptions;
    }

    @JsonProperty
    public long getPushTimeout()
    {
      return pushTimeout;
    }

    @JsonProperty
    public boolean isLogParseExceptions()
    {
      return logParseExceptions;
    }

    @JsonProperty
    public int getMaxParseExceptions()
    {
      return maxParseExceptions;
    }

    @JsonProperty
    public int getMaxSavedParseExceptions()
    {
      return maxSavedParseExceptions;
    }

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
    }

    @Nullable
    @Override
    @JsonProperty
    public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
    {
      return segmentWriteOutMediumFactory;
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
      IndexTuningConfig that = (IndexTuningConfig) o;
      return maxRowsInMemory == that.maxRowsInMemory &&
             Objects.equals(maxTotalRows, that.maxTotalRows) &&
             maxPendingPersists == that.maxPendingPersists &&
             forceGuaranteedRollup == that.forceGuaranteedRollup &&
             reportParseExceptions == that.reportParseExceptions &&
             pushTimeout == that.pushTimeout &&
             Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
             Objects.equals(numShards, that.numShards) &&
             Objects.equals(indexSpec, that.indexSpec) &&
             Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
             Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
             logParseExceptions == that.logParseExceptions &&
             maxParseExceptions == that.maxParseExceptions &&
             maxSavedParseExceptions == that.maxSavedParseExceptions;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          maxRowsPerSegment,
          maxRowsInMemory,
          maxTotalRows,
          numShards,
          indexSpec,
          basePersistDirectory,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    @Override
    public String toString()
    {
      return "IndexTuningConfig{" +
             "maxRowsPerSegment=" + maxRowsPerSegment +
             ", maxRowsInMemory=" + maxRowsInMemory +
             ", maxBytesInMemory=" + maxBytesInMemory +
             ", maxTotalRows=" + maxTotalRows +
             ", numShards=" + numShards +
             ", indexSpec=" + indexSpec +
             ", basePersistDirectory=" + basePersistDirectory +
             ", maxPendingPersists=" + maxPendingPersists +
             ", forceGuaranteedRollup=" + forceGuaranteedRollup +
             ", reportParseExceptions=" + reportParseExceptions +
             ", pushTimeout=" + pushTimeout +
             ", logParseExceptions=" + logParseExceptions +
             ", maxParseExceptions=" + maxParseExceptions +
             ", maxSavedParseExceptions=" + maxSavedParseExceptions +
             ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
             '}';
    }
  }
}
