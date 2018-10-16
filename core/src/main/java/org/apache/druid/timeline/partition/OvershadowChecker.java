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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

// TODO: maybe rename?
public class OvershadowChecker<T extends Overshadowable<T>>
{
  private static final Logger log = new Logger(OvershadowChecker.class);

  // TODO: sync all segment info periodically
  private final Map<Integer, PartitionChunk<T>> allPartitionChunks;
//  private final Int2ObjectMap<SortedSet<Integer>> overshadowedMap;

  private final Map<Integer, PartitionChunk<T>> onlineSegments;
  private final Map<Integer, PartitionChunk<T>> visibleSegments;

  public OvershadowChecker()
  {
    this.allPartitionChunks = new HashMap<>();
//    this.overshadowedMap = new Int2ObjectOpenHashMap<>();
    this.onlineSegments = new HashMap<>();
    this.visibleSegments = new HashMap<>();
  }

  public OvershadowChecker(OvershadowChecker<T> other)
  {
    this.allPartitionChunks = new HashMap<>(other.allPartitionChunks);
//    this.overshadowedMap = new Int2ObjectOpenHashMap<>(other.overshadowedMap);
    this.onlineSegments = new HashMap<>(other.onlineSegments);
    this.visibleSegments = new HashMap<>(other.visibleSegments);
  }

  public void add(PartitionChunk<T> partitionChunk)
  {
    final PartitionChunk<T> prevChunk = allPartitionChunks.put(partitionChunk.getChunkNumber(), partitionChunk);
    if (prevChunk != null) {
//      throw new ISE("Duplicate partitionId[%d]! prevChunk[%s], newChunk[%s]", partitionChunk.getChunkNumber(), prevChunk, partitionChunk);
      log.warn("prevChunk[%s] is overwritten by newChunk[%s] for partitionId[%d]", prevChunk, partitionChunk, partitionChunk.getChunkNumber());
    }

    if (!partitionChunk.getObject().getOvershadowedGroup().isEmpty()) {
      final Set<Integer> overshadowedGroup = new HashSet<>(partitionChunk.getObject().getOvershadowedGroup());
      final boolean hasAllAtomicGroupSameOvershadowedGroup = partitionChunk
          .getObject()
          .getAtomicUpdateGroup()
          .stream()
          .map(allPartitionChunks::get)
          .filter(Objects::nonNull)
          .allMatch(chunk -> overshadowedGroup.equals(new HashSet<>(chunk.getObject().getOvershadowedGroup())));

      if (!hasAllAtomicGroupSameOvershadowedGroup) {
        throw new ISE("all partitions of the same atomicUpdateGroup should have the same overshadowedGroup");
      }

//      final Set<Integer> atomicUpdateGroup = new HashSet<>(partitionChunk.getObject().getAtomicUpdateGroup());
//      final List<PartitionChunk<T>> atomicUpdateGroupChunks = overshadowedGroupToAtomicUpdateGroup.computeIfAbsent(
//          overshadowedGroup,
//          k -> new ArrayList<>()
//      );
//      final boolean hasSameAtomicUpdateGroupForSameOvershadowedGroup = atomicUpdateGroupChunks
//          .stream()
//          .allMatch(chunk -> atomicUpdateGroup.equals(new HashSet<>(chunk.getObject().getAtomicUpdateGroup())));
//      if (!hasSameAtomicUpdateGroupForSameOvershadowedGroup) {
//        throw new ISE("all partitions having the same overshadowedGroup should have the same atomicUpdateGroup");
//      }
//      atomicUpdateGroupChunks.add(partitionChunk);
    }

//    for (Integer overshadowedPartitionId : partitionChunk.getObject().getOvershadowedGroup()) {
//      overshadowedMap.computeIfAbsent(overshadowedPartitionId.intValue(), k -> new TreeSet<>())
//                     .add(partitionChunk.getChunkNumber());
//    }

    onlineSegments.put(partitionChunk.getChunkNumber(), partitionChunk);
    tryOnlineToVisible(partitionChunk);
  }

  private void tryOnlineToVisible(PartitionChunk<T> onlineSegment)
  {
    if (onlineSegment.getObject().getOvershadowedGroup().isEmpty()) {
      visibleSegments.put(onlineSegment.getChunkNumber(), onlineSegments.remove(onlineSegment.getChunkNumber()));
    } else {
      final List<Integer> atomicUpdateGroup = onlineSegment.getObject().getAtomicUpdateGroup();

      // if the entire atomicUpdateGroup are online, move them to visibleSegments.
      if (atomicUpdateGroup.stream().allMatch(onlineSegments::containsKey)) {
        atomicUpdateGroup.forEach(partitionId -> visibleSegments.put(partitionId, onlineSegments.remove(partitionId)));

        // TODO: probably a sanity check that all atomic update group have the same overshadowedGroup?

        onlineSegment.getObject().getOvershadowedGroup()
                     .stream()
                     .filter(visibleSegments::containsKey)
                     .forEach(eachPartitionId -> onlineSegments.put(eachPartitionId, visibleSegments.remove(eachPartitionId)));
      }
    }
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> partitionChunk)
  {
//    for (Integer overshadowedPartitionId : partitionChunk.getObject().getOvershadowedGroup()) {
//      final SortedSet<Integer> overshadowingPartitionIds = overshadowedMap.get(overshadowedPartitionId.intValue());
//      if (overshadowingPartitionIds == null) {
//        throw new ISE("Cannot find overshadowingPartitionIds for partition[%s]", overshadowedPartitionId);
//      }
//      if (!overshadowingPartitionIds.remove(partitionChunk.getChunkNumber())) {
//        throw new ISE("Cannot find partition[%s] from overshadowingPartitionIds[%s]", partitionChunk.getChunkNumber(), overshadowingPartitionIds);
//      }
//    }

    final PartitionChunk<T> knownChunk = allPartitionChunks.get(partitionChunk.getChunkNumber());
    if (knownChunk == null) {
      return null;
    }

    if (!knownChunk.getObject().equals(partitionChunk.getObject())) {
      throw new ISE(
          "WTH? Same partitionId[%d], but known partition[%s] differs from the input partition[%s]",
          partitionChunk.getChunkNumber(),
          knownChunk,
          partitionChunk
      );
    }

    tryVisibleToOnline(partitionChunk);
    visibleSegments.remove(partitionChunk.getChunkNumber());
    onlineSegments.remove(partitionChunk.getChunkNumber());

    return allPartitionChunks.remove(partitionChunk.getChunkNumber());
  }

  private void tryVisibleToOnline(PartitionChunk<T> offlineSegmentChunk)
  {
    if (visibleSegments.containsKey(offlineSegmentChunk.getChunkNumber())) {
      final T offlineSegment = offlineSegmentChunk.getObject();
      // find the latest visible atomic update group
      final List<Integer> overshadowedPartitionIds = offlineSegment.getOvershadowedGroup();
      final List<PartitionChunk<T>> onlineSegmentsInOvershadowedGroup = findAllOnlineSegmentsFor(overshadowedPartitionIds);

      if (!onlineSegmentsInOvershadowedGroup.isEmpty()) {
        final List<PartitionChunk<T>> latestOnlineAtomicUpdateGroup = findLatestOnlineAtomicUpdateGroup(onlineSegmentsInOvershadowedGroup);

        if (!latestOnlineAtomicUpdateGroup.isEmpty()) {
          // if found, replace the current atomicUpdateGroup with the latestOnlineAtomicUpdateGroup
          offlineSegment.getAtomicUpdateGroup()
                        .forEach(partitionId -> onlineSegments.put(partitionId, visibleSegments.remove(partitionId)));
          latestOnlineAtomicUpdateGroup.forEach(chunk -> visibleSegments.put(
              chunk.getChunkNumber(),
              onlineSegments.remove(chunk.getChunkNumber())
          ));
        }
      }
    }
  }

  // TODO: improve this: looks like the first if is unnecessary sometimes
  private List<PartitionChunk<T>> findLatestOnlineAtomicUpdateGroup(List<PartitionChunk<T>> partitionChunks)
  {
    if (partitionChunks.stream().flatMap(chunk -> chunk.getObject().getAtomicUpdateGroup().stream()).allMatch(onlineSegments::containsKey)) {
      return partitionChunks;
    } else {
      final List<Integer> allOvershadowedGroupOfChunks = partitionChunks.stream()
                                                                        .flatMap(chunk -> chunk.getObject().getOvershadowedGroup().stream())
                                                                        .collect(Collectors.toList());

      if (allOvershadowedGroupOfChunks.isEmpty()) {
        return Collections.emptyList();
      } else {
        final List<PartitionChunk<T>> onlineSegmentsInAtomicUpdateGroup = findAllOnlineSegmentsFor(allOvershadowedGroupOfChunks);

        if (!onlineSegmentsInAtomicUpdateGroup.isEmpty()) {
          return onlineSegmentsInAtomicUpdateGroup;
        } else {
          return findLatestOnlineAtomicUpdateGroup(onlineSegmentsInAtomicUpdateGroup);
        }
      }
    }
  }

  private List<PartitionChunk<T>> findAllOnlineSegmentsFor(List<Integer> partitionIds)
  {
    final List<PartitionChunk<T>> found = partitionIds.stream()
                                                      .map(onlineSegments::get)
                                                      .filter(Objects::nonNull)
                                                      .collect(Collectors.toList());
    return found.size() == partitionIds.size() ? found : Collections.emptyList();
  }

  public boolean isEmpty()
  {
    return visibleSegments.isEmpty();
  }

  public boolean isComplete()
  {
    // TODO
    return true;
  }

  public boolean isVisible(PartitionChunk<T> partitionChunk)
  {
    // TODO
    return false;
  }

  @Nullable
  public PartitionChunk<T> getChunk(int partitionId)
  {
    return visibleSegments.get(partitionId);
  }

  public SortedSet<PartitionChunk<T>> findVisibles()
  {
    return new TreeSet<>(visibleSegments.values());
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
    OvershadowChecker<?> that = (OvershadowChecker<?>) o;
    return Objects.equals(allPartitionChunks, that.allPartitionChunks) &&
//           Objects.equals(overshadowedMap, that.overshadowedMap) &&
           Objects.equals(onlineSegments, that.onlineSegments) &&
           Objects.equals(visibleSegments, that.visibleSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allPartitionChunks, onlineSegments, visibleSegments);
  }

  @Override
  public String toString()
  {
    return "OvershadowChecker{" +
           "allPartitionChunks=" + allPartitionChunks +
//           ", overshadowedMap=" + overshadowedMap +
           ", onlineSegments=" + onlineSegments +
           ", visibleSegments=" + visibleSegments +
           '}';
  }
}
