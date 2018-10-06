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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

public class DataSegmentOvershadowChecker implements OvershadowChecker<DataSegment>
{
  // TODO: sync all segment info periodically
  private final Map<DataSegment, PartitionChunk<DataSegment>> allPartitionChunks;
  private final Int2ObjectMap<SortedSet<Integer>> overshadowedMap;

  private final Map<Integer, DataSegment> onlineSegments;
  private final Map<Integer, DataSegment> visibleSegments;

  public DataSegmentOvershadowChecker()
  {
    this.allPartitionChunks = new HashMap<>();
    this.overshadowedMap = new Int2ObjectOpenHashMap<>();
    this.onlineSegments = new HashMap<>();
    this.visibleSegments = new HashMap<>();
  }

  private DataSegmentOvershadowChecker(DataSegmentOvershadowChecker other)
  {
    this.allPartitionChunks = new HashMap<>(other.allPartitionChunks);
    this.overshadowedMap = new Int2ObjectOpenHashMap<>(other.overshadowedMap);
    this.onlineSegments = new HashMap<>(other.onlineSegments);
    this.visibleSegments = new HashMap<>(other.visibleSegments);
  }

  @Override
  public OvershadowChecker<DataSegment> copy()
  {
    return new DataSegmentOvershadowChecker(this);
  }

  @Override
  public void add(PartitionChunk<DataSegment> partitionChunk)
  {
    allPartitionChunks.put(partitionChunk.getObject(), partitionChunk);
    for (Integer overshadowedPartitionId : partitionChunk.getObject().getOvershadowedGroup()) {
      overshadowedMap.computeIfAbsent(overshadowedPartitionId.intValue(), k -> new TreeSet<>())
                     .add(partitionChunk.getChunkNumber());
    }

    final DataSegment segment = partitionChunk.getObject();
    onlineSegments.put(segment.getShardSpec().getPartitionNum(), segment);
    tryOnlineToVisible(segment);
  }

  private void tryOnlineToVisible(DataSegment onlineSegment)
  {
    final List<Integer> atomicUpdateGroup = onlineSegment.getAtomicUpdateGroup();

    // if atomicUpdateGroup is empty or all of them are online, move them to visibleSegments.
    // allMatch should be used to check the former case.
    if (atomicUpdateGroup.stream().allMatch(onlineSegments::containsKey)) {
      atomicUpdateGroup.forEach(partitionId -> visibleSegments.put(partitionId, onlineSegments.remove(partitionId)));

      // TODO: probably a sanity check that all atomic update group have the same overshadowedGroup?

      onlineSegment.getOvershadowedGroup()
                   .stream()
                   .filter(visibleSegments::containsKey)
                   .forEach(eachPartitionId -> onlineSegments.put(eachPartitionId, visibleSegments.remove(eachPartitionId)));
    }
  }

  @Nullable
  @Override
  public PartitionChunk<DataSegment> remove(PartitionChunk<DataSegment> partitionChunk)
  {
    for (Integer overshadowedPartitionId : partitionChunk.getObject().getOvershadowedGroup()) {
      final SortedSet<Integer> overshadowingPartitionIds = overshadowedMap.get(overshadowedPartitionId.intValue());
      if (overshadowingPartitionIds == null) {
        throw new ISE("Cannot find overshadowingPartitionIds for partition[%s]", overshadowedPartitionId);
      }
      if (!overshadowingPartitionIds.remove(partitionChunk.getChunkNumber())) {
        throw new ISE("Cannot find partition[%s] from overshadowingPartitionIds[%s]", partitionChunk.getChunkNumber(), overshadowingPartitionIds);
      }
    }

    tryVisibleToOnline(partitionChunk.getObject());
    visibleSegments.remove(partitionChunk.getChunkNumber());
    onlineSegments.remove(partitionChunk.getChunkNumber());

    return allPartitionChunks.remove(partitionChunk.getObject());
  }

  @Override
  public boolean isEmpty()
  {
    return visibleSegments.isEmpty();
  }

  private void tryVisibleToOnline(DataSegment offlineSegment)
  {
    if (visibleSegments.containsKey(offlineSegment.getShardSpec().getPartitionNum())) {
      // find the latest visible atomic update group
      final List<Integer> overshadowedPartitionIds = offlineSegment.getOvershadowedGroup();
      final DataSegment onlineSegmentInOvershadowedGroup = findAnyOnlineSegmentFrom(overshadowedPartitionIds);
      final List<Integer> latestOnlineAtomicUpdateGroup = findLatestOnlineAtomicUpdateGroup(
          onlineSegmentInOvershadowedGroup.getAtomicUpdateGroup()
      );

      if (!latestOnlineAtomicUpdateGroup.isEmpty()) {
        // if found, replace the current atomicUpdateGroup with the latestOnlineAtomicUpdateGroup
        offlineSegment.getAtomicUpdateGroup().forEach(partitionId -> onlineSegments.put(partitionId, visibleSegments.remove(partitionId)));
        latestOnlineAtomicUpdateGroup.forEach(partitionId -> visibleSegments.put(partitionId, onlineSegments.remove(partitionId)));
      }
    }
  }

  private List<Integer> findLatestOnlineAtomicUpdateGroup(List<Integer> atomicUpdateGroup)
  {
    if (atomicUpdateGroup.stream().allMatch(onlineSegments::containsKey)) {
      return atomicUpdateGroup;
    } else {
      // TODO: probably a sanity check that all atomic update group have the same overshadowedGroup?
      final DataSegment onlineSegmentInAtomicUpdateGroup = findAnyOnlineSegmentFrom(atomicUpdateGroup);

      if (onlineSegmentInAtomicUpdateGroup != null) {
        final DataSegment onlineSegmentInOvershadowedGroup = findAnyOnlineSegmentFrom(onlineSegmentInAtomicUpdateGroup.getOvershadowedGroup());
        if (onlineSegmentInOvershadowedGroup != null) {
          return findLatestOnlineAtomicUpdateGroup(onlineSegmentInOvershadowedGroup.getAtomicUpdateGroup());
        }
      }
      return Collections.emptyList();
    }
  }

  @Nullable
  private DataSegment findAnyOnlineSegmentFrom(List<Integer> partitionIds)
  {
    return partitionIds.stream().map(onlineSegments::get).filter(Objects::nonNull).findAny().orElse(null);
  }

  @Override
  public boolean isComplete()
  {
    // TODO
    return false;
  }

  @Override
  public boolean isVisible(PartitionChunk<DataSegment> partitionChunk)
  {
    return false;
  }

  @Nullable
  @Override
  public PartitionChunk<DataSegment> getChunk(int partitionId)
  {
    final DataSegment segment = visibleSegments.get(partitionId);
    return segment == null ? null : allPartitionChunks.get(segment);
  }

  @Override
  public SortedSet<PartitionChunk<DataSegment>> findVisibles()
  {
    final SortedSet<PartitionChunk<DataSegment>> visibleChunks = new TreeSet<>();
    visibleSegments.values().stream().map(allPartitionChunks::get).forEach(visibleChunks::add);
    return visibleChunks;
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
    DataSegmentOvershadowChecker that = (DataSegmentOvershadowChecker) o;
    return Objects.equals(allPartitionChunks, that.allPartitionChunks) &&
           Objects.equals(overshadowedMap, that.overshadowedMap) &&
           Objects.equals(onlineSegments, that.onlineSegments) &&
           Objects.equals(visibleSegments, that.visibleSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allPartitionChunks, overshadowedMap, onlineSegments, visibleSegments);
  }

  @Override
  public String toString()
  {
    return "DataSegmentOvershadowChecker{" +
           "allPartitionChunks=" + allPartitionChunks +
           ", overshadowedMap=" + overshadowedMap +
           ", onlineSegments=" + onlineSegments +
           ", visibleSegments=" + visibleSegments +
           '}';
  }
}
