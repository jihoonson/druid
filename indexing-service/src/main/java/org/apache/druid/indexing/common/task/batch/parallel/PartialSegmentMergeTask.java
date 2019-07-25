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
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.impl.prefetch.Fetchers;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class PartialSegmentMergeTask extends AbstractBatchIndexTask
{
  public static final String TYPE = "partial_index_merge";

  private static final int BUFFER_SIZE = 1024 * 4;

  private final byte[] buffer = new byte[BUFFER_SIZE];
  private final int numAttempts;
  private final PartialSegmentMergeIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexTaskClient> taskClientFactory;

  @JsonCreator
  public PartialSegmentMergeTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final PartialSegmentMergeIngestionSpec ingestionSchema,
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
  public PartialSegmentMergeIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
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
        "This method should be never called because PartialSegmentMergeTask always uses timeChunk locking"
        + " but this method is supposed to be called only with segment locking."
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
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    // TODO: lock check and fail if it's revoked?
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final File tempDir = toolbox.getFirehoseTemporaryDir();
    for (PartitionLocation partitionLocation : ingestionSchema.getIOConfig().getPartitionLocations()) {
      final URI uri = partitionLocation.toIntermediaryDataServerURI(supervisorTaskId);
      final File destFile = FileUtils.getFile(tempDir, )
      Fetchers.fetch(uri.toURL().openStream(), )
    }
    return null;
  }
}
