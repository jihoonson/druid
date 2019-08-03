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

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;

@RunWith(Parameterized.class)
public class MultiPhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private final LockGranularity lockGranularity;
  private File inputDir;

  public MultiPhaseParallelIndexingTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
  }

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 24 + i, i));
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    indexingServiceClient = new LocalIndexingServiceClient();
    localDeepStorage = temporaryFolder.newFolder("localStorage");
  }

  @After
  public void teardown()
  {
    indexingServiceClient.shutdown();
    temporaryFolder.delete();
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    TestSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(id, taskResource, ingestionSchema, context, indexingServiceClient);
    }

    @Override
    public PartialSegmentGenerateParallelIndexTaskRunner createPartialSegmentGenerateRunner(TaskToolbox toolbox)
    {
      return new TestPartialSegmentGenerateRunner(toolbox, this, getIndexingServiceClient());
    }
  }

  private static class TestPartialSegmentGenerateRunner extends PartialSegmentGenerateParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestPartialSegmentGenerateRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    Iterator<SubTaskSpec<PartialSegmentGenerateTask>> subTaskSpecIterator() throws IOException
    {
      final Iterator<SubTaskSpec<PartialSegmentGenerateTask>> iterator = super.subTaskSpecIterator();
      return new Iterator<SubTaskSpec<PartialSegmentGenerateTask>>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public SubTaskSpec<PartialSegmentGenerateTask> next()
        {
          try {
            Thread.sleep(10);
            return iterator.next();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    @Override
    SubTaskSpec<PartialSegmentGenerateTask> newTaskSpec(InputSplit split)
    {
      final ParallelIndexIngestionSpec subTaskIngestionSpec = new ParallelIndexIngestionSpec(
          getIngestionSchema().getDataSchema(),
          new ParallelIndexIOConfig(
              getBaseFirehoseFactory().withSplit(split),
              getIngestionSchema().getIOConfig().isAppendToExisting()
          ),
          getIngestionSchema().getTuningConfig()
      );
      return new SubTaskSpec<PartialSegmentGenerateTask>(
          getTaskId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          getTaskId(),
          getContext(),
          split
      )
      {
        @Override
        public PartialSegmentGenerateTask newSubTask(int numAttempts)
        {
          return new PartialSegmentGenerateTask(
              null,
              getGroupId(),
              null,
              getSupervisorTaskId(),
              numAttempts,
              subTaskIngestionSpec,
              getContext(),
              null,
              new LocalParallelIndexTaskClientFactory(supervisorTask)
          );
        }
      };
    }
  }
}
