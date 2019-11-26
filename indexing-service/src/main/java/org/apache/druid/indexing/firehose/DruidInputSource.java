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

package org.apache.druid.indexing.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class DruidInputSource extends AbstractInputSource implements SplittableInputSource<List<WindowedSegmentId>>
{
  private final String dataSource;
  // Exactly one of interval and segmentIds should be non-null. Typically 'interval' is specified directly
  // by the user creating this firehose and 'segmentIds' is used for sub-tasks if it is split for parallel
  // batch ingestion.
  @Nullable
  private final Interval interval;
  @Nullable
  private final List<WindowedSegmentId> segmentIds;
  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final List<String> metrics;
  @Nullable
  private final Long maxInputSegmentBytesPerTask;
  private final IndexIO indexIO;
  private final CoordinatorClient coordinatorClient;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final RetryPolicyFactory retryPolicyFactory;
  private final DruidSegmentInputFormat inputFormat;

  @JsonCreator
  public DruidInputSource(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Nullable Interval interval,
      // Specifying "segments" is intended only for when this FirehoseFactory has split itself,
      // not for direct end user use.
      @JsonProperty("segments") @Nullable List<WindowedSegmentId> segmentIds,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("maxInputSegmentBytesPerTask") @Deprecated @Nullable Long maxInputSegmentBytesPerTask,
      @JacksonInject IndexIO indexIO,
      @JacksonInject CoordinatorClient coordinatorClient,
      @JacksonInject SegmentLoaderFactory segmentLoaderFactory,
      @JacksonInject RetryPolicyFactory retryPolicyFactory
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    if ((interval == null && segmentIds == null) || (interval != null && segmentIds != null)) {
      throw new IAE("Specify exactly one of 'interval' and 'segments'");
    }
    this.dataSource = dataSource;
    this.interval = interval;
    this.segmentIds = segmentIds;
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.maxInputSegmentBytesPerTask = maxInputSegmentBytesPerTask;
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
    this.coordinatorClient = Preconditions.checkNotNull(coordinatorClient, "null CoordinatorClient");
    this.segmentLoaderFactory = Preconditions.checkNotNull(segmentLoaderFactory, "null SegmentLoaderFactory");
    this.retryPolicyFactory = Preconditions.checkNotNull(retryPolicyFactory, "null RetryPolicyFactory");
    this.inputFormat = new DruidSegmentInputFormat(indexIO, dimFilter);
  }

  @Override
  protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
  {
    final SegmentLoader segmentLoader = segmentLoaderFactory.manufacturate(temporaryDirectory);
    final Stream<InputEntity> entityStream = createSplits(inputFormat, null)
        .flatMap(split -> {
          final List<TimelineObjectHolder<String, DataSegment>> holders = createTimeline(split);
          return holders
              .stream()
              .flatMap(holder -> {
                final PartitionHolder<DataSegment> partitionHolder = holder.getObject();
                return partitionHolder
                    .stream()
                    .map(chunk -> new DruidSegmentEntity(segmentLoader, chunk.getObject(), holder.getInterval()));
              });
        });

    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        entityStream,
        temporaryDirectory
    );
  }

  private List<TimelineObjectHolder<String, DataSegment>> createTimeline(InputSplit<List<WindowedSegmentId>> split)
  {
    final List<DataSegment> segments = new ArrayList<>();
    for (WindowedSegmentId segmentId : Preconditions.checkNotNull(split.get())) {
      final DataSegment segment = coordinatorClient.getDatabaseSegmentDataSourceSegment(
          dataSource,
          segmentId.getSegmentId()
      );
      segments.add(segment);
    }
    return VersionedIntervalTimeline.forSegments(segments).lookup(Intervals.ETERNITY);
  }

  @Override
  public Stream<InputSplit<List<WindowedSegmentId>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    // See IngestSegmentFirehoseFactory.initializeSplitsIfNeeded
    return null;
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return 0;
  }

  @Override
  public SplittableInputSource<List<WindowedSegmentId>> withSplit(InputSplit<List<WindowedSegmentId>> split)
  {
    return new DruidInputSource(
        dataSource,
        null,
        split.get(),
        dimFilter,
        dimensions,
        metrics,
        maxInputSegmentBytesPerTask,
        indexIO,
        coordinatorClient,
        segmentLoaderFactory,
        retryPolicyFactory
    );
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }
}
