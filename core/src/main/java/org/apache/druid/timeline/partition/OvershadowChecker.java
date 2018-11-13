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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.PartitionChunkProvider;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

// TODO: maybe rename?
public class OvershadowChecker<T extends Overshadowable<T>>
{
  private static final Logger log = new Logger(OvershadowChecker.class);

  private final Map<Integer, PartitionChunk<T>> knownPartitionChunks;
  private final PartitionChunkProvider<T> extraPartitionProvider;
//  private final Int2ObjectMap<SortedSet<Integer>> overshadowedMap;

  private final Map<Integer, PartitionChunk<T>> onlineSegments;
  private final Map<Integer, PartitionChunk<T>> visibleSegments;

  public OvershadowChecker(PartitionChunkProvider<T> extraPartitionProvider)
  {
    this.knownPartitionChunks = new HashMap<>();
    this.extraPartitionProvider = extraPartitionProvider;
//    this.overshadowedMap = new Int2ObjectOpenHashMap<>();
    this.onlineSegments = new HashMap<>();
    this.visibleSegments = new HashMap<>();
  }

  public OvershadowChecker(OvershadowChecker<T> other)
  {
    this.knownPartitionChunks = new HashMap<>(other.knownPartitionChunks);
    this.extraPartitionProvider = other.extraPartitionProvider;
//    this.overshadowedMap = new Int2ObjectOpenHashMap<>(other.overshadowedMap);
    this.onlineSegments = new HashMap<>(other.onlineSegments);
    this.visibleSegments = new HashMap<>(other.visibleSegments);
  }

  private Map<Integer, PartitionChunkCandidate> buildAllOnlineCandidates()
  {
    final Map<Integer, PartitionChunkCandidate> allPartitionChunks = new HashMap<>(toCandidates(onlineSegments, true));
    allPartitionChunks.putAll(toCandidates(extraPartitionProvider.get(), false));
    return allPartitionChunks;
  }

  private Map<Integer, PartitionChunkCandidate> toCandidates(Map<Integer, PartitionChunk<T>> map, boolean online)
  {
    return map.entrySet().stream().collect(
        Collectors.toMap(Entry::getKey, entry -> new PartitionChunkCandidate(entry.getValue(), online))
    );
  }

  public void add(PartitionChunk<T> partitionChunk)
  {
    final PartitionChunk<T> prevChunk = knownPartitionChunks.put(partitionChunk.getChunkNumber(), partitionChunk);
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
          .map(knownPartitionChunks::get)
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
      final Set<Integer> atomicUpdateGroup = onlineSegment.getObject().getAtomicUpdateGroup();

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

    final PartitionChunk<T> knownChunk = knownPartitionChunks.get(partitionChunk.getChunkNumber());
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

    return knownPartitionChunks.remove(partitionChunk.getChunkNumber());
  }

  private void tryVisibleToOnline(PartitionChunk<T> offlineSegmentChunk)
  {
    if (visibleSegments.containsKey(offlineSegmentChunk.getChunkNumber())) {
      final T offlineSegment = offlineSegmentChunk.getObject();
      // find the latest visible atomic update group
      final Set<Integer> overshadowedPartitionIds = offlineSegment.getOvershadowedGroup();
      final List<PartitionChunkCandidate> onlineSegmentsInOvershadowedGroup = findAllOnlineSegmentsFor(overshadowedPartitionIds);

      if (!onlineSegmentsInOvershadowedGroup.isEmpty()) {
        final List<PartitionChunkCandidate> latestOnlineAtomicUpdateGroup = findLatestOnlineAtomicUpdateGroup(onlineSegmentsInOvershadowedGroup);

        // sanity check
        Preconditions.checkState(
            latestOnlineAtomicUpdateGroup.stream().allMatch(candidate -> candidate.online),
            "WTH? entire latestOnlineAtomicUpdateGroup should be online"
        );

        if (!latestOnlineAtomicUpdateGroup.isEmpty()) {
          // if found, replace the current atomicUpdateGroup with the latestOnlineAtomicUpdateGroup
          offlineSegment.getAtomicUpdateGroup()
                        .forEach(partitionId -> onlineSegments.put(partitionId, visibleSegments.remove(partitionId)));
          latestOnlineAtomicUpdateGroup.forEach(candidate -> visibleSegments.put(
              candidate.chunk.getChunkNumber(),
              onlineSegments.remove(candidate.chunk.getChunkNumber())
          ));
        }
      }
    }
  }

  // TODO: improve this: looks like the first if is unnecessary sometimes
  private List<PartitionChunkCandidate> findLatestOnlineAtomicUpdateGroup(List<PartitionChunkCandidate> candidates)
  {
    if (candidates.stream().allMatch(candidate -> candidate.online)) {
      return candidates;
    } else {
      final Set<Integer> allOvershadowedGroupOfChunks = candidates.stream()
                                                                  .flatMap(candidate -> candidate.chunk.getObject().getOvershadowedGroup().stream())
                                                                  .collect(Collectors.toSet());

      if (allOvershadowedGroupOfChunks.isEmpty()) {
        return Collections.emptyList();
      } else {
        return findLatestOnlineAtomicUpdateGroup(findAllOnlineSegmentsFor(allOvershadowedGroupOfChunks));
      }
    }
  }

  private List<PartitionChunkCandidate> findAllOnlineSegmentsFor(Set<Integer> partitionIds)
  {
    final Map<Integer, PartitionChunkCandidate> candidates = buildAllOnlineCandidates();
    final List<PartitionChunkCandidate> found = partitionIds.stream()
//                                                      .map(onlineSegments::get)
                                                            .map(candidates::get)
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

  public SortedSet<PartitionChunk<T>> getVisibles()
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
    return Objects.equals(knownPartitionChunks, that.knownPartitionChunks) &&
//           Objects.equals(overshadowedMap, that.overshadowedMap) &&
           Objects.equals(onlineSegments, that.onlineSegments) &&
           Objects.equals(visibleSegments, that.visibleSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(knownPartitionChunks, onlineSegments, visibleSegments);
  }

  @Override
  public String toString()
  {
    return "OvershadowChecker{" +
           "knownPartitionChunks=" + knownPartitionChunks +
//           ", overshadowedMap=" + overshadowedMap +
           ", onlineSegments=" + onlineSegments +
           ", visibleSegments=" + visibleSegments +
           '}';
  }

  private class PartitionChunkCandidate
  {
    private final PartitionChunk<T> chunk;
    private final boolean online;

    private PartitionChunkCandidate(PartitionChunk<T> chunk, boolean online)
    {
      this.chunk = chunk;
      this.online = online;
    }
  }
}
