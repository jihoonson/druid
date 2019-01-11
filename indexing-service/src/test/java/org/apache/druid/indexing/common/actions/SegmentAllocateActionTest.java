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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SegmentAllocateActionTest
{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final String DATA_SOURCE = "none";
  private static final DateTime PARTY_TIME = DateTimes.of("1999");
  private static final DateTime THE_DISTANT_FUTURE = DateTimes.of("3000");

  @Before
  public void setUp()
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);
  }

  @Test
  public void testGranularitiesFinerThanDay()
  {
    Assert.assertEquals(
        ImmutableList.of(
            Granularities.DAY,
            Granularities.SIX_HOUR,
            Granularities.HOUR,
            Granularities.THIRTY_MINUTE,
            Granularities.FIFTEEN_MINUTE,
            Granularities.TEN_MINUTE,
            Granularities.FIVE_MINUTE,
            Granularities.MINUTE,
            Granularities.SECOND
        ),
        Granularity.granularitiesFinerThan(Granularities.DAY)
    );
  }

  @Test
  public void testGranularitiesFinerThanHour()
  {
    Assert.assertEquals(
        ImmutableList.of(
            Granularities.HOUR,
            Granularities.THIRTY_MINUTE,
            Granularities.FIFTEEN_MINUTE,
            Granularities.TEN_MINUTE,
            Granularities.FIVE_MINUTE,
            Granularities.MINUTE,
            Granularities.SECOND
        ),
        Granularity.granularitiesFinerThan(Granularities.HOUR)
    );
  }

  @Test
  public void testManySegmentsSameInterval()
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id2.getIdentifierAsString()
    );

    final List<TaskLock> partyTimeLocks = taskActionTestKit.getTaskLockbox()
                                                           .findLocksForTask(task)
                                                           .stream()
                                                           .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                           .collect(Collectors.toList());

    Assert.assertEquals(3, partyTimeLocks.size());

    for (TaskLock partyLock : partyTimeLocks) {
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id1
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id2
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(2, 0)
          ),
          id3
      );
    }
  }

  @Test
  public void testResumeSequence()
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    Assert.assertNotNull(id1);
    final SegmentIdentifier id2 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    Assert.assertNotNull(id2);
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id2.getIdentifierAsString()
    );
    Assert.assertNotNull(id3);
    final SegmentIdentifier id4 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    Assert.assertNull(id4);
    final SegmentIdentifier id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    Assert.assertNotNull(id5);
    final SegmentIdentifier id6 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.MINUTE,
        "s1",
        id1.getIdentifierAsString()
    );
    Assert.assertNull(id6);
    final SegmentIdentifier id7 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.DAY,
        "s1",
        id1.getIdentifierAsString()
    );
    Assert.assertNotNull(id7);

    final List<TaskLock> partyLocks = taskActionTestKit.getTaskLockbox()
                                                       .findLocksForTask(task)
                                                       .stream()
                                                       .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                       .collect(Collectors.toList());

    Assert.assertEquals(2, partyLocks.size());

    for (TaskLock partyLock : partyLocks) {
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id1
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id3
      );
    }

    final List<TaskLock> futureLocks = taskActionTestKit
        .getTaskLockbox()
        .findLocksForTask(task)
        .stream()
        .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
        .collect(Collectors.toList());

    Assert.assertEquals(3, futureLocks.size());

    for (TaskLock futureLock : futureLocks) {
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id2
      );
    }

    Assert.assertNull(id4);
    assertSameIdentifier(id2, id5);
    Assert.assertNull(id6);
    assertSameIdentifier(id2, id7);
  }

  @Test
  public void testMultipleSequences()
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s2",
        null
    );
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id4 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id3.getIdentifierAsString()
    );
    final SegmentIdentifier id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s2",
        id2.getIdentifierAsString()
    );
    final SegmentIdentifier id6 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );

    final List<TaskLock> partyLocks = taskActionTestKit.getTaskLockbox()
                                                       .findLocksForTask(task)
                                                       .stream()
                                                       .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                       .collect(Collectors.toList());

    Assert.assertEquals(4, partyLocks.size());

    for (TaskLock partyLock : partyLocks) {
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id1
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id2
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(2, 0)
          ),
          id3
      );
    }

    final List<TaskLock> futureLocks = taskActionTestKit
        .getTaskLockbox()
        .findLocksForTask(task)
        .stream()
        .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
        .collect(Collectors.toList());

    Assert.assertEquals(2, futureLocks.size());

    for (TaskLock futureLock : futureLocks) {
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id4
      );
      assertSameIdentifier(
          new SegmentIdentifier(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id5
      );
    }

    assertSameIdentifier(
        id1,
        id6
    );
  }

  @Test
  public void testAddToExistingLinearShardSpecsSameGranularity() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(0))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(1))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );

    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(2)
        ),
        id1
    );
    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(3)
        ),
        id2
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsSameGranularity() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );

    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(3, 2)
        ),
        id2
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsCoarserPreferredGranularity() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.DAY,
        "s1",
        null
    );

    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsFinerPreferredGranularity() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.MINUTE,
        "s1",
        null
    );

    assertSameIdentifier(
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
  }

  @Test
  public void testCannotAddToExistingNumberedShardSpecsWithCoarserQueryGranularity() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.DAY,
        Granularities.DAY,
        "s1",
        null
    );

    Assert.assertNull(id1);
  }

  @Test
  public void testCannotDoAnythingWithSillyQueryGranularity()
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.DAY,
        Granularities.HOUR,
        "s1",
        null
    );

    Assert.assertNull(id1);
  }

  @Test
  public void testCannotAddToExistingSingleDimensionShardSpecs() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new SingleDimensionShardSpec("foo", null, "bar", 0))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new SingleDimensionShardSpec("foo", "bar", null, 1))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );

    Assert.assertNull(id1);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(HashBasedNumberedShardSpecFactory.class);

    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        PARTY_TIME,
        Granularities.MINUTE,
        Granularities.HOUR,
        "s1",
        "prev",
        false,
        Collections.emptySet()
    );

    final SegmentAllocateAction action2 = (SegmentAllocateAction) objectMapper.readValue(
        objectMapper.writeValueAsBytes(action),
        TaskAction.class
    );

    Assert.assertEquals(action.getDataSource(), action2.getDataSource());
    Assert.assertEquals(action.getTimestamp(), action2.getTimestamp());
    Assert.assertEquals(action.getQueryGranularity(), action2.getQueryGranularity());
    Assert.assertEquals(action.getPreferredSegmentGranularity(), action2.getPreferredSegmentGranularity());
    Assert.assertEquals(action.getSequenceName(), action2.getSequenceName());
    Assert.assertEquals(action.getPreviousSegmentId(), action2.getPreviousSegmentId());
    Assert.assertEquals(action.isSkipSegmentLineageCheck(), action2.isSkipSegmentLineageCheck());
    Assert.assertEquals(action.getOvershadowingSegments(), action2.getOvershadowingSegments());
  }

  @Test
  public void testWithShardSpecFactoryAndOvershadowingSegments() throws IOException
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final ObjectMapper objectMapper = new DefaultObjectMapper();

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new HashBasedNumberedShardSpec(0, 2, ImmutableList.of("dim1"), objectMapper))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new HashBasedNumberedShardSpec(1, 2, ImmutableList.of("dim1"), objectMapper))
                       .build()
        )
    );

    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        PARTY_TIME,
        Granularities.MINUTE,
        Granularities.HOUR,
        "seq",
        null,
        true,
        ImmutableSet.of(0, 1)
    );
    final SegmentIdentifier segmentIdentifier = action.perform(task, taskActionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(segmentIdentifier);

    final ShardSpec shardSpec = segmentIdentifier.getShardSpec();
    Assert.assertEquals(2, shardSpec.getPartitionNum());

    Assert.assertTrue(shardSpec instanceof HashBasedNumberedShardSpec);
    final HashBasedNumberedShardSpec hashBasedNumberedShardSpec = (HashBasedNumberedShardSpec) shardSpec;
    Assert.assertEquals(0, hashBasedNumberedShardSpec.getOrdinal());
    Assert.assertEquals(3, hashBasedNumberedShardSpec.getPartitions());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2"), hashBasedNumberedShardSpec.getPartitionDimensions());

    Assert.assertEquals(ImmutableSet.of(0, 1), segmentIdentifier.getOvershadowingSegments());
  }

  private SegmentIdentifier allocate(
      final Task task,
      final DateTime timestamp,
      final Granularity queryGranularity,
      final Granularity preferredSegmentGranularity,
      final String sequenceName,
      final String sequencePreviousId
  )
  {
    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        timestamp,
        queryGranularity,
        preferredSegmentGranularity,
        sequenceName,
        sequencePreviousId,
        false,
        Collections.emptySet()
    );
    return action.perform(task, taskActionTestKit.getTaskActionToolbox());
  }

  private void assertSameIdentifier(final SegmentIdentifier expected, final SegmentIdentifier actual)
  {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.getShardSpec().getPartitionNum(), actual.getShardSpec().getPartitionNum());
    Assert.assertEquals(expected.getShardSpec().getClass(), actual.getShardSpec().getClass());

    if (expected.getShardSpec().getClass() == NumberedShardSpec.class
        && actual.getShardSpec().getClass() == NumberedShardSpec.class) {
      Assert.assertEquals(
          ((NumberedShardSpec) expected.getShardSpec()).getPartitions(),
          ((NumberedShardSpec) actual.getShardSpec()).getPartitions()
      );
    } else if (expected.getShardSpec().getClass() == LinearShardSpec.class
               && actual.getShardSpec().getClass() == LinearShardSpec.class) {
      // do nothing
    }
  }
}
