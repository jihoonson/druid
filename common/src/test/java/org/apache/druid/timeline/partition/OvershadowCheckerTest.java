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
package org.apache.druid.timeline.partition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.OvershadowableInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

public class OvershadowCheckerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private OvershadowChecker<OvershadowableInteger> overshadowChecker;

  @Before
  public void setup()
  {
    overshadowChecker = new OvershadowChecker<>();
  }

  @Test
  public void testAddWithSingleAtomicUpdateGroup()
  {
    overshadowChecker.add(newChunk(0, 0));
    Assert.assertEquals(
        ImmutableSet.of(newChunk(0, 0)),
        overshadowChecker.findVisibles()
    );
    overshadowChecker.add(newChunk(1, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0)
        ),
        overshadowChecker.findVisibles()
    );
    overshadowChecker.add(newChunk(2, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );
  }

  @Test
  public void testAddWithLargeAtomicUpdateGroup()
  {
    final List<Integer> atomicUpdateGroup = ImmutableList.of(0, 1, 2);
    overshadowChecker.add(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup)),
        overshadowChecker.findVisibles()
    );
    overshadowChecker.add(chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );
    overshadowChecker.add(chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );
  }

  @Test
  public void testDuplicatePartitionId()
  {
    overshadowChecker.add(newChunk(0, 0));

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Duplicate partitionId[0]");
    overshadowChecker.add(newChunk(0, 1));
  }

  @Test
  public void testOvershadowWithDifferentAtomicUpdateGroup()
  {
    overshadowChecker.add(newChunk(0, 0));
    overshadowChecker.add(newChunk(1, 0));
    overshadowChecker.add(newChunk(2, 0));

    final List<Integer> overshadowedGroup = ImmutableList.of(0, 1);
    overshadowChecker.add(chunkWithOvershadowedGroup(3, 1, overshadowedGroup));
    expectedException.expect(ISE.class);
    expectedException.expectMessage("all partitions having the same overshadowedGroup should have");
    overshadowChecker.add(chunkWithOvershadowedGroup(4, 1, overshadowedGroup));
  }

  @Test
  public void testOvershadowWithSameAtomicUpdateGroup()
  {
    overshadowChecker.add(newChunk(0, 0));
    overshadowChecker.add(newChunk(1, 0));
    overshadowChecker.add(newChunk(2, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    final List<Integer> overshadowedGroup = ImmutableList.of(0, 1);
    final List<Integer> atomicUpdateGroup = ImmutableList.of(3, 4);
    overshadowChecker.add(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    overshadowChecker.add(newChunk(5, 1));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0),
            newChunk(5, 1)
        ),
        overshadowChecker.findVisibles()
    );

    overshadowChecker.add(newChunk(4, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(2, 0),
            newChunk(3, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(4, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(5, 1)
        ),
        overshadowChecker.findVisibles()
    );
  }

  @Test
  public void testRemoveWithSingleAtomicUpdateGroup()
  {
    overshadowChecker.add(newChunk(0, 0));
    overshadowChecker.add(newChunk(1, 0));
    overshadowChecker.add(newChunk(2, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(newChunk(1, 0), overshadowChecker.remove(newChunk(1, 0)));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(newChunk(0, 0), overshadowChecker.remove(newChunk(0, 0)));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(newChunk(2, 0), overshadowChecker.remove(newChunk(2, 0)));
    Assert.assertEquals(
        ImmutableSet.of(),
        overshadowChecker.findVisibles()
    );

    Assert.assertNull(overshadowChecker.remove(newChunk(2, 0)));
  }

  @Test
  public void testRemoveWithLargeAtomicUpdateGroup()
  {
    final List<Integer> atomicUpdateGroup = ImmutableList.of(0, 1, 2);
    overshadowChecker.add(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup));
    overshadowChecker.add(chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup));
    overshadowChecker.add(chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup),
        overshadowChecker.remove(chunkWithAtomicUpdateGroup(1, 0, atomicUpdateGroup))
    );
    Assert.assertEquals(
        ImmutableSet.of(
            chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup),
            chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup),
        overshadowChecker.remove(chunkWithAtomicUpdateGroup(2, 0, atomicUpdateGroup))
    );
    Assert.assertEquals(
        ImmutableSet.of(
            chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup),
        overshadowChecker.remove(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup))
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        overshadowChecker.findVisibles()
    );

    Assert.assertNull(overshadowChecker.remove(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup)));
  }

  @Test
  public void testRemoveUnknownPartition()
  {
    final List<Integer> atomicUpdateGroup = ImmutableList.of(0, 1, 2);
    overshadowChecker.add(chunkWithAtomicUpdateGroup(0, 0, atomicUpdateGroup));

    expectedException.expect(ISE.class);
    expectedException.expectMessage("WTH? Same partitionId[0], but known partition");
    overshadowChecker.remove(newChunk(0, 0));
  }

  @Test
  public void testRemoveOnlinePartition()
  {
    overshadowChecker.add(newChunk(0, 0));
    overshadowChecker.add(newChunk(1, 0));
    overshadowChecker.add(newChunk(2, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    final List<Integer> overshadowedGroup = ImmutableList.of(0, 1);
    final List<Integer> atomicUpdateGroup = ImmutableList.of(3, 4);
    overshadowChecker.add(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        newChunk(3, 1, overshadowedGroup, atomicUpdateGroup),
        overshadowChecker.remove(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup))
    );

    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    overshadowChecker.add(newChunk(4, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );
  }

  @Test
  public void testFallBackOnRemove()
  {
    overshadowChecker.add(newChunk(0, 0));
    overshadowChecker.add(newChunk(1, 0));
    overshadowChecker.add(newChunk(2, 0));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    final List<Integer> overshadowedGroup = ImmutableList.of(0, 1);
    final List<Integer> atomicUpdateGroup = ImmutableList.of(3, 4, 5);
    overshadowChecker.add(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup));
    overshadowChecker.add(newChunk(4, 1, overshadowedGroup, atomicUpdateGroup));
    overshadowChecker.add(newChunk(5, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(2, 0),
            newChunk(3, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(4, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(5, 1, overshadowedGroup, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        newChunk(3, 1, overshadowedGroup, atomicUpdateGroup),
        overshadowChecker.remove(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup))
    );
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );

    overshadowChecker.add(newChunk(3, 1, overshadowedGroup, atomicUpdateGroup));
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(2, 0),
            newChunk(3, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(4, 1, overshadowedGroup, atomicUpdateGroup),
            newChunk(5, 1, overshadowedGroup, atomicUpdateGroup)
        ),
        overshadowChecker.findVisibles()
    );

    Assert.assertEquals(
        newChunk(5, 1, overshadowedGroup, atomicUpdateGroup),
        overshadowChecker.remove(newChunk(5, 1, overshadowedGroup, atomicUpdateGroup))
    );
    Assert.assertEquals(
        ImmutableSet.of(
            newChunk(0, 0),
            newChunk(1, 0),
            newChunk(2, 0)
        ),
        overshadowChecker.findVisibles()
    );
  }

  private static NumberedPartitionChunk<OvershadowableInteger> newChunk(
      int partitionId,
      int val
  )
  {
    return new NumberedPartitionChunk<>(partitionId, 0, new OvershadowableInteger(partitionId, val));
  }

  private static NumberedPartitionChunk<OvershadowableInteger> chunkWithOvershadowedGroup(
      int partitionId,
      int val,
      List<Integer> overshadowedGroup
  )
  {
    return newChunk(
        partitionId,
        val,
        overshadowedGroup,
        Collections.singletonList(partitionId)
    );
  }

  private static NumberedPartitionChunk<OvershadowableInteger> chunkWithAtomicUpdateGroup(
      int partitionId,
      int val,
      List<Integer> atomicUpdateGroup
  )
  {
    return newChunk(
        partitionId,
        val,
        Collections.emptyList(),
        atomicUpdateGroup
    );
  }

  private static NumberedPartitionChunk<OvershadowableInteger> newChunk(
      int partitionId,
      int val,
      List<Integer> overshadowGroup,
      List<Integer> atomicUpdateGroup
  )
  {
    return new NumberedPartitionChunk<>(
        partitionId,
        val,
        new OvershadowableInteger(partitionId, val, overshadowGroup, atomicUpdateGroup)
    );
  }
}
