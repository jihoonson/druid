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
package org.apache.druid.benchmark.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.PostgreSQLMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnectorConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class PendingSegmentAllocationBenchmark
{
  private static final String DATASOURCE_PREFIX = "dataSource";
  private static final String SEQUENCE_PREFIX = "sequence";
  private static final Interval INTERVAL = Intervals.of("2019-01-01/2020-01-01");

  @Param({"test"})
  private String tableNamePrefix;

  @Param({"jdbc:postgresql://localhost:5432/druid"})
  private String uri;

  private final String user = "jihoon";

  private final String password = "";

  @Param({"8"})
  private int numThreads;

  @Param({"40"})
  private int numSegmentsToAllocate;

  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskLockbox taskLockbox;
  private ExecutorService executorService;
  private List<Task> tasks;

  @Setup
  public void setup() throws InterruptedException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(NumberedShardSpec.class);

    final MetadataStorageTablesConfig storageTablesConfig = new MetadataStorageTablesConfig(
        tableNamePrefix,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    final MetadataStorageConnectorConfig storageConnectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return uri;
      }

      @Override
      public String getUser()
      {
        return user;
      }

      @Override
      public String getPassword()
      {
        return password;
      }
    };

    final SQLMetadataConnector metadataConnector = new PostgreSQLConnector(
        () -> storageConnectorConfig,
        () -> storageTablesConfig,
        new PostgreSQLConnectorConfig()
    );

    metadataConnector.createPendingSegmentsTable();
    metadataConnector.createSegmentTable();
    metadataConnector.createLockTable(StringUtils.format("%s_%s", tableNamePrefix, "tasklocks"), "task");
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(objectMapper, storageTablesConfig, metadataConnector);

    final TaskStorage taskStorage = new MetadataTaskStorage(
        metadataConnector,
        new TaskStorageConfig(null),
        new PostgreSQLMetadataStorageActionHandlerFactory(metadataConnector, storageTablesConfig, objectMapper)
    );
    taskLockbox = new TaskLockbox(taskStorage);
    tasks = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final Task task = new BenchTask(StringUtils.format("id_%d", i), "sameGroup", DATASOURCE_PREFIX);
      tasks.add(task);
      taskLockbox.add(task);
      final LockResult result = taskLockbox.lock(TaskLockType.EXCLUSIVE, task, INTERVAL);
      if (!result.isOk()) {
        throw new ISE("Failed to get a lock for task[%s]", task.getId());
      }
    }

    executorService = Execs.multiThreaded(numThreads, "pending-segment-allocation-benchmark-%d");
  }

  @TearDown
  public void tearDown()
  {
    executorService.shutdown();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void allocatePendingSegments(Blackhole blackhole) throws ExecutionException, InterruptedException
  {
    final List<Future<Void>> futures = IntStream.range(0, numThreads)
                                                .mapToObj(i -> run(i, blackhole))
                                                .collect(Collectors.toList());

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void allocatePendingSegmentsInCriticalSection(Blackhole blackhole) throws ExecutionException, InterruptedException
  {
    final List<Future<Void>> futures = IntStream.range(0, numThreads)
                                                .mapToObj(i -> runInCriticalSection(i, blackhole))
                                                .collect(Collectors.toList());

    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private Future<Void> run(int index, Blackhole blackhole)
  {
    return executorService.submit(() -> {
      for (int i = 0; i < numSegmentsToAllocate; i++) {
        final SegmentIdentifier id = metadataStorageCoordinator.allocatePendingSegment(
            DATASOURCE_PREFIX,
            StringUtils.format("%s_%d_%d", SEQUENCE_PREFIX, index, i),
            null,
            INTERVAL,
            DateTimes.nowUtc().toString(),
            true
        );
        blackhole.consume(id);
      }
      return null;
    });
  }

  private Future<Void> runInCriticalSection(int index, Blackhole blackhole)
  {
    return executorService.submit(() -> {
      for (int i = 0; i < numSegmentsToAllocate; i++) {
        final int sufix = i;
        final SegmentIdentifier id = taskLockbox.doInCriticalSection(
            tasks.get(index),
            Collections.singletonList(INTERVAL),
            CriticalAction.<SegmentIdentifier>builder()
                .onValidLocks(() -> metadataStorageCoordinator.allocatePendingSegment(
                    DATASOURCE_PREFIX,
                    StringUtils.format("%s_%d_%d", SEQUENCE_PREFIX, index, sufix),
                    null,
                    INTERVAL,
                    DateTimes.nowUtc().toString(),
                    true
                ))
                .onInvalidLocks(() -> null)
                .build()
        );
        blackhole.consume(id);
      }
      return null;
    });
  }

  private static class BenchTask implements Task
  {
    private final String id;
    private final String groupId;
    private final String dataSource;

    private BenchTask(String id, String groupId, String dataSource)
    {
      this.id = id;
      this.groupId = groupId;
      this.dataSource = dataSource;
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public String getGroupId()
    {
      return groupId;
    }

    @Override
    public TaskResource getTaskResource()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getNodeType()
    {
      return null;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      return null;
    }

    @Override
    public String getClasspathPrefix()
    {
      return null;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public boolean canRestore()
    {
      return false;
    }

    @Override
    public void stopGracefully()
    {

    }

    @Override
    public TaskStatus run(TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    public Map<String, Object> getContext()
    {
      return Collections.emptyMap();
    }
  }
}
