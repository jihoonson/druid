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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionBucketAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.PartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.RangePartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.RangePartitionBucketAnalysis;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NonLinearlyPartitionedSequenceNameFunctionTest
{
  private static final String TASKID = "taskid";
  private static final Interval INTERVAL_EMPTY = Intervals.utc(0, 1000);
  private static final Interval INTERVAL_SINGLETON = Intervals.utc(1000, 2000);
  private static final Interval INTERVAL_NORMAL = Intervals.utc(2000, 3000);

  private static final String PARTITION_DIMENSION = "dimension";
  private static final String PARTITION0 = "0";
  private static final String PARTITION5 = "5";
  private static final String PARTITION9 = "9";

  private static final PartitionBoundaries EMPTY_PARTITIONS = PartitionBoundaries.empty();
  private static final PartitionBoundaries SINGLETON_PARTITIONS = PartitionBoundaries.fromNonNullBoundaries(
      new String[]{
          PARTITION0,
          PARTITION0
      }
  );
  private static final PartitionBoundaries NORMAL_PARTITIONS = PartitionBoundaries.fromNonNullBoundaries(
      new String[]{
          PARTITION0,
          PARTITION5,
          PARTITION9
      }
  );

  private static final Map<Interval, PartitionBoundaries> INTERVAL_TO_PARTITONS = ImmutableMap.of(
      INTERVAL_EMPTY, EMPTY_PARTITIONS,
      INTERVAL_SINGLETON, SINGLETON_PARTITIONS,
      INTERVAL_NORMAL, NORMAL_PARTITIONS
  );

  @Test
  public void testGetSequenceNameWithRangePartitionAnalysisReturningValidSequenceName()
  {
    final RangePartitionAnalysis partitionAnalysis = new RangePartitionAnalysis(
        new SingleDimensionPartitionsSpec(null, 1, PARTITION_DIMENSION, false)
    );
    INTERVAL_TO_PARTITONS.forEach((interval, partitionBoundaries) -> partitionAnalysis.updateBucket(
        interval,
        new RangePartitionBucketAnalysis(PARTITION_DIMENSION, partitionBoundaries)
    ));
    assertGetSequenceName(partitionAnalysis, 1);
  }

  @Test
  public void testGetSequenceNameWithHashRangePartitionAnalysisReturning()
  {
    final int numBuckets = 3;
    final List<String> partitionDimensions = Collections.singletonList(PARTITION_DIMENSION);
    final HashPartitionAnalysis partitionAnalysis = new HashPartitionAnalysis(
        new HashedPartitionsSpec(
            null,
            numBuckets,
            partitionDimensions
        )
    );
    INTERVAL_TO_PARTITONS.forEach((interval, partitionBoundaries) -> partitionAnalysis.updateBucket(
        interval,
        new HashPartitionBucketAnalysis(partitionDimensions, numBuckets)
    ));
    assertGetSequenceName(partitionAnalysis, 0);
  }

  private void assertGetSequenceName(PartitionAnalysis partitionAnalysis, int expectedBucketId)
  {
    SequenceNameFunction sequenceNameFunction = new NonLinearlyPartitionedSequenceNameFunction(
        TASKID,
        partitionAnalysis
    );

    Interval interval = INTERVAL_NORMAL;
    InputRow row = createInputRow(interval, PARTITION9);
    String sequenceName = sequenceNameFunction.getSequenceName(interval, row);
    String expectedSequenceName = StringUtils.format("%s_%s_%d", TASKID, interval, expectedBucketId);
    Assert.assertEquals(expectedSequenceName, sequenceName);
  }

  private static InputRow createInputRow(Interval interval, String dimensionValue)
  {
    long timestamp = interval.getStartMillis();
    InputRow inputRow = EasyMock.mock(InputRow.class);
    EasyMock.expect(inputRow.getTimestamp()).andStubReturn(DateTimes.utc(timestamp));
    EasyMock.expect(inputRow.getTimestampFromEpoch()).andStubReturn(timestamp);
    EasyMock.expect(inputRow.getDimension(PARTITION_DIMENSION))
            .andStubReturn(Collections.singletonList(dimensionValue));
    EasyMock.replay(inputRow);
    return inputRow;
  }
}
