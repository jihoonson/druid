/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TaskTestBase
{
  private static int segmentAllocatePartitionCounter;

  static final TaskActionClient actionClient = new TaskActionClient()
  {
    private final List<DataSegment> segments = new ArrayList<>();

    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
    {
      if (taskAction instanceof LockListAction) {
        return (RetType) Collections.singletonList(
            new TaskLock(
                TaskLockType.EXCLUSIVE,
                "",
                "",
                Intervals.of("2014/P1Y"), DateTimes.nowUtc().toString(),
                Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
            )
        );
      }

      if (taskAction instanceof LockAcquireAction) {
        return (RetType) new TaskLock(
            TaskLockType.EXCLUSIVE, "groupId",
            "test",
            ((LockAcquireAction) taskAction).getInterval(),
            DateTimes.nowUtc().toString(),
            Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
        );
      }

      if (taskAction instanceof LockTryAcquireAction) {
        return (RetType) new TaskLock(
            TaskLockType.EXCLUSIVE,
            "groupId",
            "test",
            ((LockTryAcquireAction) taskAction).getInterval(),
            DateTimes.nowUtc().toString(),
            Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
        );
      }

      if (taskAction instanceof SegmentTransactionalInsertAction) {
        final SegmentTransactionalInsertAction action = (SegmentTransactionalInsertAction) taskAction;
        segments.addAll(action.getSegments());
        return (RetType) new SegmentPublishResult(action.getSegments(), true);
      }

      if (taskAction instanceof SegmentAllocateAction) {
        SegmentAllocateAction action = (SegmentAllocateAction) taskAction;
        Interval interval = action.getPreferredSegmentGranularity().bucket(action.getTimestamp());
        ShardSpec shardSpec = new NumberedShardSpec(segmentAllocatePartitionCounter++, 0);
        return (RetType) new SegmentIdentifier(action.getDataSource(), interval, "latestVersion", shardSpec);
      }

      if (taskAction instanceof SegmentListUsedAction) {
        return (RetType) segments;
      }

      return null;
    }
  };

  static final File BASE_DIR = Files.createTempDir();

  static final TestUtils testUtils = new TestUtils(initializeJsonMapper());

  static final SegmentLoaderFactory segmentLoaderFactory = new SegmentLoaderFactory(
      new SegmentLoaderLocalCacheManager(
          testUtils.getTestIndexIO(),
          new SegmentLoaderConfig().withLocations(ImmutableList.of()),
          testUtils.getTestObjectMapper()
      )
  );

  private static ObjectMapper initializeJsonMapper()
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    for (Module module : new LocalDataStorageDruidModule().getJacksonModules()) {
      jsonMapper.registerModule(module);
    }
    return jsonMapper;
  }

  static List<DataSegment> runTask(Task task) throws Exception
  {
    final List<DataSegment> segments = new ArrayList<>();
    final File taskDir = new File(BASE_DIR, task.getId());
    final File segmentsDir = new File(taskDir, "segments");
    final DataSegmentPusher pusher = new LocalDataSegmentPusher(
        new LocalDataSegmentPusherConfig(segmentsDir),
        testUtils.getTestObjectMapper()
    )
    {
      @Override
      public DataSegment push(File file, DataSegment segment) throws IOException
      {
        final DataSegment pushed = super.push(file, segment);
        segments.add(pushed);
        return pushed;
      }
    };

    final SegmentLoader segmentLoader = segmentLoaderFactory.manufacturate(segmentsDir);

    final TaskToolbox toolbox = new TaskToolbox(
        new TaskConfig(BASE_DIR.getAbsolutePath(), null, null, 50000, null, false, null, null),
        actionClient,
        null,
        pusher,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        segmentLoader,
        testUtils.getTestObjectMapper(),
        null,
        testUtils.getTestIndexIO(),
        null,
        null,
        testUtils.getTestIndexMergerV9(),
        null,
        null,
        null,
        null
    );

    task.isReady(toolbox.getTaskActionClient());
    task.run(toolbox);

    segments.sort(null);

    return segments;
  }

  static IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return new IndexTask.IndexIngestionSpec(
        new DataSchema(
            "test",
            testUtils.getTestObjectMapper().convertValue(
                new StringInputRowParser(
                    Preconditions.checkNotNull(parseSpec),
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            granularitySpec != null ? granularitySpec : new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                Arrays.asList(Intervals.of("2014/2015"))
            ),
            testUtils.getTestObjectMapper()
        ),
        new IndexTask.IndexIOConfig(
            new LocalFirehoseFactory(
                baseDir,
                "druid*",
                null
            ),
            appendToExisting
        ),
        tuningConfig
    );
  }

  static IndexTuningConfig createTuningConfig(
      Integer targetPartitionSize,
      Integer numShards,
      boolean forceExtendableShardSpecs,
      boolean forceGuaranteedRollup
  )
  {
    return createTuningConfig(
        targetPartitionSize,
        1,
        null,
        numShards,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        true
    );
  }

  static IndexTuningConfig createTuningConfig(
      Integer targetPartitionSize,
      Integer maxRowsInMemory,
      Integer maxTotalRows,
      Integer numShards,
      boolean forceExtendableShardSpecs,
      boolean forceGuaranteedRollup,
      boolean reportParseException
  )
  {
    return new IndexTask.IndexTuningConfig(
        targetPartitionSize,
        maxRowsInMemory,
        maxTotalRows,
        null,
        numShards,
        null,
        null,
        true,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseException,
        null
    );
  }
}
