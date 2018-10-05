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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockTryAcquireAction;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskRunner.SubTaskSpecStatus;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ParallelIndexSupervisorTask is capable of running multiple subTasks for parallel indexing. This is
 * applicable if the input {@link FiniteFirehoseFactory} is splittable. While this task is running, it can submit
 * multiple child tasks to overlords. This task succeeds only when all its child tasks succeed; otherwise it fails.
 *
 * @see ParallelIndexTaskRunner
 */
public class ParallelIndexSupervisorTask extends AbstractTask implements ChatHandler
{
  public static final String TYPE = "index_parallel";

  private static final Logger log = new Logger(ParallelIndexSupervisorTask.class);

  private final ParallelIndexIngestionSpec ingestionSchema;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final IndexingServiceClient indexingServiceClient;
  private final ChatHandlerProvider chatHandlerProvider;
  private final AuthorizerMapper authorizerMapper;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  private final Counters counters = new Counters();

  private volatile ParallelIndexTaskRunner runner;

  // toolbox is initlized when run() is called, and can be used for processing HTTP endpoint requests.
  private volatile TaskToolbox toolbox;

  private final Map<Interval, List<Integer>> inputSegmentPartitionIds = new HashMap<>();

  @JsonCreator
  public ParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject @Nullable IndexingServiceClient indexingServiceClient, // null in overlords
      @JacksonInject @Nullable ChatHandlerProvider chatHandlerProvider,     // null in overlords
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        null,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context
    );

    this.ingestionSchema = ingestionSchema;

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    if (!(firehoseFactory instanceof FiniteFirehoseFactory)) {
      throw new IAE("[%s] should implement FiniteFirehoseFactory", firehoseFactory.getClass().getSimpleName());
    }

    this.baseFirehoseFactory = (FiniteFirehoseFactory) firehoseFactory;
    this.indexingServiceClient = indexingServiceClient;
    this.chatHandlerProvider = chatHandlerProvider;
    this.authorizerMapper = authorizerMapper;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;

    if (ingestionSchema.getTuningConfig().getMaxSavedParseExceptions() > 0) {
      log.warn("maxSavedParseExceptions is not supported yet");
    }
    if (ingestionSchema.getTuningConfig().getMaxParseExceptions() > 0) {
      log.warn("maxParseExceptions is not supported yet");
    }
    if (ingestionSchema.getTuningConfig().isLogParseExceptions()) {
      log.warn("logParseExceptions is not supported yet");
    }
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

  @JsonProperty("spec")
  public ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  @Nullable
  ParallelIndexTaskRunner getRunner()
  {
    return runner;
  }

  @VisibleForTesting
  AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  @VisibleForTesting
  ParallelIndexTaskRunner createRunner(TaskToolbox toolbox)
  {
    if (ingestionSchema.getTuningConfig().isForceGuaranteedRollup()) {
      throw new UnsupportedOperationException("Perfect roll-up is not supported yet");
    } else {
      runner = new SinglePhaseParallelIndexTaskRunner(
          toolbox,
          getId(),
          getGroupId(),
          ingestionSchema,
          getContext(),
          indexingServiceClient
      );
    }
    return runner;
  }

  @VisibleForTesting
  void setRunner(ParallelIndexTaskRunner runner)
  {
    this.runner = runner;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
//    final Optional<SortedSet<Interval>> intervals = ingestionSchema.getDataSchema()
//                                                                   .getGranularitySpec()
//                                                                   .bucketIntervals();
//
//    return !intervals.isPresent() || checkLock(taskActionClient);
    return checkLock(taskActionClient);
  }

  private boolean checkLock(TaskActionClient actionClient) throws IOException
  {
    final LockGranularity lockGranularity = findRequiredLockGranularity(actionClient);

    if (lockGranularity != null) {
      for (Entry<Interval, List<Integer>> entry : inputSegmentPartitionIds.entrySet()) {
        final TaskLock lock = actionClient.submit(
            new LockTryAcquireAction(lockGranularity, TaskLockType.EXCLUSIVE, entry.getKey(), entry.getValue())
        );
        if (lock == null) {
          return false;
        }
      }
    }
    return true;
  }

  @Nullable
  private LockGranularity findRequiredLockGranularity(TaskActionClient actionClient) throws IOException
  {
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      final List<DataSegment> segments = initInputSegmentPartitionIds(actionClient);

      if (segments.isEmpty()) {
        throw new ISE("empty input segments");
      }

      final Granularity segmentGranularity = ingestionSchema.getDataSchema().getGranularitySpec().getSegmentGranularity();
      if (!segmentGranularity.match(segments.get(0).getInterval())) {
        return LockGranularity.TIME_CHUNK;
      } else {
        return LockGranularity.SEGMENT;
      }
    } else {
      return null;
    }
  }

  private List<DataSegment> initInputSegmentPartitionIds(TaskActionClient actionClient) throws IOException
  {
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      final Interval interval = ((IngestSegmentFirehoseFactory) firehoseFactory).getInterval();
      final List<DataSegment> segments = actionClient.submit(
          new SegmentListUsedAction(getDataSource(), null, Collections.singletonList(interval))
      );

      for (DataSegment segment : segments) {
        inputSegmentPartitionIds.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>())
                                .add(segment.getShardSpec().getPartitionNum());
      }
      return segments;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    setToolbox(toolbox);
    initInputSegmentPartitionIds(toolbox.getTaskActionClient());

    log.info(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(chatHandlerProvider, "chatHandlerProvider").getClass().getName()
    );
    chatHandlerProvider.register(getId(), this, false);

    try {
      if (baseFirehoseFactory.isSplittable()) {
        return runParallel(toolbox);
      } else {
        log.warn(
            "firehoseFactory[%s] is not splittable. Running sequentially",
            baseFirehoseFactory.getClass().getSimpleName()
        );
        return runSequential(toolbox);
      }
    }
    finally {
      chatHandlerProvider.unregister(getId());
    }
  }

  @VisibleForTesting
  void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  private TaskStatus runParallel(TaskToolbox toolbox) throws Exception
  {
    createRunner(toolbox);
    return TaskStatus.fromCode(getId(), runner.run());
  }

  private TaskStatus runSequential(TaskToolbox toolbox)
  {
    return new IndexTask(
        getId(),
        getGroupId(),
        getTaskResource(),
        getDataSource(),
        new IndexIngestionSpec(
            getIngestionSchema().getDataSchema(),
            getIngestionSchema().getIOConfig(),
            convertToIndexTuningConfig(getIngestionSchema().getTuningConfig())
        ),
        getContext(),
        authorizerMapper,
        chatHandlerProvider,
        rowIngestionMetersFactory
    ).run(toolbox);
  }

  private static IndexTuningConfig convertToIndexTuningConfig(ParallelIndexTuningConfig tuningConfig)
  {
    return new IndexTuningConfig(
        tuningConfig.getTargetPartitionSize(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemory(),
        tuningConfig.getMaxTotalRows(),
        null,
        tuningConfig.getNumShards(),
        tuningConfig.getIndexSpec(),
        tuningConfig.getMaxPendingPersists(),
        true,
        tuningConfig.isForceExtendableShardSpecs(),
        false,
        tuningConfig.isReportParseExceptions(),
        null,
        tuningConfig.getPushTimeout(),
        tuningConfig.getSegmentWriteOutMediumFactory(),
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
  }

  // Internal APIs

//  /**
//   * Allocate a new {@link SegmentIdentifier} for a request from {@link ParallelIndexSubTask}.
//   * The returned segmentIdentifiers have different {@code partitionNum} (thereby different {@link NumberedShardSpec})
//   * per bucket interval.
//   */
//  @POST
//  @Path("/segment/allocate")
//  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
//  public Response allocateSegment(
//      DateTime timestamp,
//      @Context final HttpServletRequest req
//  )
//  {
//    ChatHandlers.authorizationCheck(
//        req,
//        Action.READ,
//        getDataSource(),
//        authorizerMapper
//    );
//
//    if (toolbox == null) {
//      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
//    }
//
//    try {
//      final SegmentIdentifier segmentIdentifier = allocateNewSegment(timestamp);
//      return Response.ok(toolbox.getObjectMapper().writeValueAsBytes(segmentIdentifier)).build();
//    }
//    catch (IOException | IllegalStateException e) {
//      return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
//    }
//    catch (IllegalArgumentException e) {
//      return Response.status(Response.Status.BAD_REQUEST).entity(Throwables.getStackTraceAsString(e)).build();
//    }
//  }

//  @VisibleForTesting
//  SegmentIdentifier allocateNewSegment(DateTime timestamp) throws IOException
//  {
//    final String dataSource = getDataSource();
//    final GranularitySpec granularitySpec = getIngestionSchema().getDataSchema().getGranularitySpec();
//    final SortedSet<Interval> bucketIntervals = Preconditions.checkNotNull(
//        granularitySpec.bucketIntervals().orNull(),
//        "bucketIntervals"
//    );
////    // List locks whenever allocating a new segment because locks might be revoked and no longer valid.
////    final Map<Interval, String> versions = toolbox
////        .getTaskActionClient()
////        .submit(new LockListAction())
////        .stream()
////        .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
//
//    final Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
//    if (!maybeInterval.isPresent()) {
//      throw new IAE("Could not find interval for timestamp [%s]", timestamp);
//    }
//
//    final Interval interval = maybeInterval.get();
//    if (!bucketIntervals.contains(interval)) {
//      throw new ISE("Unspecified interval[%s] in granularitySpec[%s]", interval, granularitySpec);
//    }
//
//    final LockResult lockResult = toolbox.getTaskActionClient().submit(
//        new LockTryAcquireForNewSegmentAction(
//            TaskLockType.EXCLUSIVE,
//            interval,
//            getId() // TODO: proper sequence name??
//        )
//    );
//
////    final int partitionNum = counters.increment(interval.toString(), 1);
//    if (lockResult.isRevoked()) {
//      throw new ISE("Lock resovked for interval[%s]", interval);
//    }
//    if (lockResult.isOk()) {
//      return new SegmentIdentifier(
//          dataSource,
//          interval,
//          lockResult.getTaskLock().getVersion(),
//          new NumberedShardSpec(((SegmentLock) lockResult.getTaskLock()).getPartitionIds().get(0), 0),
//          inputSegmentPartitionIds.get(interval)
//      );
//    } else {
//      throw new ISE("Failed to get a lock for interval[%s] with sequenceName[%s]", interval, getId());
//    }
//  }

  private static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Entry::getValue)
                   .findFirst()
                   .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }

  /**
   * {@link ParallelIndexSubTask}s call this API to report the segments they've generated and pushed.
   */
  @POST
  @Path("/report")
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  public Response report(
      PushedSegmentsReport report,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(
        req,
        Action.WRITE,
        getDataSource(),
        authorizerMapper
    );
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      runner.collectReport(report);
      return Response.ok().build();
    }
  }

  // External APIs to get running status

  @GET
  @Path("/mode")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMode(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(baseFirehoseFactory.isSplittable() ? "parallel" : "sequential").build();
    }
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getProgress()).build();
    }
  }

  @GET
  @Path("/subtasks/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getRunningTaskIds()).build();
    }
  }

  @GET
  @Path("/subtaskspecs")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getRunningSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/complete")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getCompleteSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspec/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpec(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpec subTaskSpec = runner.getSubTaskSpec(id);
      if (subTaskSpec == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskSpec).build();
      }
    }
  }

  @GET
  @Path("/subtaskspec/{id}/state")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskState(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpecStatus subTaskSpecStatus = runner.getSubTaskState(id);
      if (subTaskSpecStatus == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskSpecStatus).build();
      }
    }
  }

  @GET
  @Path("/subtaskspec/{id}/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecAttemptHistory(
      @PathParam("id") String id,
      @Context final HttpServletRequest req
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final TaskHistory taskHistory = runner.getCompleteSubTaskSpecAttemptHistory(id);
      if (taskHistory == null) {
        return Response.status(Status.NOT_FOUND).build();
      } else {
        return Response.ok(taskHistory.getAttemptHistory()).build();
      }
    }
  }
}
