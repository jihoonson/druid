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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.PartitionChunkProvider;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: maybe rename?
// TODO: bulk build

/**
 * Not thread-safe
 * @param <T>
 */
public class OvershadowableManagerPerTimeChunk<T extends Overshadowable<T>>
{
  private static final Logger log = new Logger(OvershadowableManagerPerTimeChunk.class);

  private enum State
  {
    STANDBY,
    VISIBLE,
    OVERSHADOWED
  }

  private static final Set<Pair<State, State>> DEFINED_TRANSITIONS = ImmutableSet.of(
      Pair.of(State.STANDBY, State.VISIBLE),
      Pair.of(State.STANDBY, State.OVERSHADOWED),
      Pair.of(State.VISIBLE, State.OVERSHADOWED),
      Pair.of(State.OVERSHADOWED, State.VISIBLE)
  );

  private final Map<Integer, PartitionChunk<T>> knownPartitionChunks;
  private final PartitionChunkProvider<T> extraPartitionProvider;

  // standby -> visible <-> overshadowed
  private final Map<Integer, PartitionChunk<T>> standbySegments;
  private final Map<Integer, PartitionChunk<T>> visibleSegments;
  private final Map<Integer, PartitionChunk<T>> overshadowedSegments;

  public OvershadowableManagerPerTimeChunk(PartitionChunkProvider<T> extraPartitionProvider)
  {
    this.knownPartitionChunks = new HashMap<>();
    this.extraPartitionProvider = extraPartitionProvider;
    this.standbySegments = new HashMap<>();
    this.visibleSegments = new HashMap<>();
    this.overshadowedSegments = new HashMap<>();
  }

  public OvershadowableManagerPerTimeChunk(OvershadowableManagerPerTimeChunk<T> other)
  {
    this.knownPartitionChunks = new HashMap<>(other.knownPartitionChunks);
    this.extraPartitionProvider = other.extraPartitionProvider;
    this.standbySegments = new HashMap<>(other.standbySegments);
    this.visibleSegments = new HashMap<>(other.visibleSegments);
    this.overshadowedSegments = new HashMap<>(other.overshadowedSegments);
  }

  private Map<Integer, PartitionChunkCandidate> buildAllOnlineCandidates()
  {
    final Map<Integer, PartitionChunkCandidate> allPartitionChunks = new HashMap<>(toCandidates(standbySegments, true));
    allPartitionChunks.putAll(toCandidates(extraPartitionProvider.get(), false));
    return allPartitionChunks;
  }

  private Map<Integer, PartitionChunkCandidate> toCandidates(Map<Integer, PartitionChunk<T>> map, boolean online)
  {
    return map.entrySet().stream().collect(
        Collectors.toMap(Entry::getKey, entry -> new PartitionChunkCandidate(entry.getValue(), online))
    );
  }

  private Map<Integer, PartitionChunk<T>> getSegmentMapOf(State state)
  {
    switch (state) {
      case STANDBY:
        return standbySegments;
      case VISIBLE:
        return visibleSegments;
      case OVERSHADOWED:
        return overshadowedSegments;
      default:
        throw new ISE("Unknown state[%s]", state);
    }
  }

  private void transitPartitionChunkState(int partitionChunkNumber, State from, State to)
  {
    final PartitionChunk<T> partitionChunk = getSegmentMapOf(from).get(partitionChunkNumber);
    transitPartitionChunkState(partitionChunk, from, to);
  }

  private void transitPartitionChunkState(PartitionChunk<T> partitionChunk, State from, State to)
  {
    Preconditions.checkNotNull(partitionChunk, "partitionChunk");
    if (!DEFINED_TRANSITIONS.contains(Pair.of(from, to))) {
      throw new ISE("Invalid state transition from [%s] to [%s]", from, to);
    }
    final Map<Integer, PartitionChunk<T>> fromMap = getSegmentMapOf(from);
    final Map<Integer, PartitionChunk<T>> toMap = getSegmentMapOf(to);
    toMap.put(
        partitionChunk.getChunkNumber(),
        Preconditions.checkNotNull(
            fromMap.remove(partitionChunk.getChunkNumber()),
            "partitionChunk[%s] is not %s state",
            partitionChunk,
            from
        )
    );
  }

  private Stream<PartitionChunk<T>> streamOf(State... states)
  {
    return Arrays.stream(states).flatMap(state -> getSegmentMapOf(state).values().stream());
  }

  private Stream<PartitionChunk<T>> partitionChunksOf(Set<Integer> partitionIds)
  {
    return partitionIds.stream().map(knownPartitionChunks::get).filter(Objects::nonNull);
  }

  public void add(PartitionChunk<T> partitionChunk)
  {
    final PartitionChunk<T> prevChunk = knownPartitionChunks.put(partitionChunk.getChunkNumber(), partitionChunk);
    if (prevChunk != null) {
      log.warn(
          "prevChunk[%s] is overwritten by newChunk[%s] for partitionId[%d]",
          prevChunk,
          partitionChunk,
          partitionChunk.getChunkNumber()
      );
    }

    // Decide an initial state of the given chunk.
    final boolean overshadowed = knownPartitionChunks
        .values()
        .stream()
        .anyMatch(chunk -> chunk.getObject().isOvershadow(partitionChunk.getObject()));

    if (overshadowed) {
      overshadowedSegments.put(partitionChunk.getChunkNumber(), partitionChunk);
    } else {
      final Set<Integer> overshadowedGroup = partitionChunk.getObject().getOvershadowedGroup();
      if (!overshadowedGroup.isEmpty()) {
        final Set<Integer> atomicUpdateGroup = partitionChunk.getObject().getAtomicUpdateGroup();

        if (!atomicUpdateGroup.isEmpty()) {
          // Check all segments in the atomicUpdateGroup have the same overshadowedGroup.

          final boolean allAtomicUpdateGroupHaveSameOvershadowedGroup = partitionChunksOf(atomicUpdateGroup)
              .allMatch(chunk -> overshadowedGroup.equals(chunk.getObject().getOvershadowedGroup()));

          if (!allAtomicUpdateGroupHaveSameOvershadowedGroup) {
            throw new ISE("all partitions of the same atomicUpdateGroup should have the same overshadowedGroup");
          }
        }
      }

      standbySegments.put(partitionChunk.getChunkNumber(), partitionChunk);
      tryStandbyToVisible(partitionChunk);
    }
  }

  private void tryStandbyToVisible(PartitionChunk<T> standbySegment)
  {
    final Set<Integer> overshadowedGroupIds = standbySegment.getObject().getOvershadowedGroup();
    if (overshadowedGroupIds.isEmpty()) {
      transitPartitionChunkState(standbySegment, State.STANDBY, State.VISIBLE);
    } else {
      final Set<Integer> atomicUpdateGroupIds = standbySegment.getObject().getAtomicUpdateGroup();

      // if the entire atomicUpdateGroup are online, move them to visibleSegments.
      if (atomicUpdateGroupIds.stream().allMatch(standbySegments::containsKey)) {
        final List<PartitionChunk<T>> atomicUpdateGroup;
        if (atomicUpdateGroupIds.isEmpty()) {
          atomicUpdateGroup = Collections.singletonList(standbySegment);
        } else {
          atomicUpdateGroup = atomicUpdateGroupIds.stream().map(standbySegments::get).collect(Collectors.toList());
        }

        atomicUpdateGroup.forEach(partition -> transitPartitionChunkState(partition, State.STANDBY, State.VISIBLE));

        // TODO: probably a sanity check that all atomic update group have the same overshadowedGroup?

        standbySegment.getObject().getOvershadowedGroup()
                     .stream()
                     .filter(visibleSegments::containsKey)
                     .forEach(eachPartitionId -> overshadowedSegments.put(eachPartitionId, visibleSegments.remove(eachPartitionId)));
      }
    }
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> partitionChunk)
  {
    final PartitionChunk<T> knownChunk = knownPartitionChunks.get(partitionChunk.getChunkNumber());
    if (knownChunk == null) {
      return null;
    }

    if (!knownChunk.equals(partitionChunk)) {
      throw new ISE(
          "WTH? Same partitionId[%d], but known partition[%s] differs from the input partition[%s]",
          partitionChunk.getChunkNumber(),
          knownChunk,
          partitionChunk
      );
    }

    tryVisibleToOnline(partitionChunk);
    visibleSegments.remove(partitionChunk.getChunkNumber());
    standbySegments.remove(partitionChunk.getChunkNumber());

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
          offlineSegment.getNonEmptyAtomicUpdateGroup(offlineSegmentChunk.getChunkNumber())
                        .forEach(partitionId -> standbySegments.put(partitionId, visibleSegments.remove(partitionId)));
          latestOnlineAtomicUpdateGroup.forEach(candidate -> visibleSegments.put(
              candidate.chunk.getChunkNumber(),
              standbySegments.remove(candidate.chunk.getChunkNumber())
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

  public Collection<PartitionChunk<T>> getOvershadowed()
  {
    return overshadowedSegments.values();
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
    OvershadowableManagerPerTimeChunk<?> that = (OvershadowableManagerPerTimeChunk<?>) o;
    return Objects.equals(knownPartitionChunks, that.knownPartitionChunks) &&
           Objects.equals(standbySegments, that.standbySegments) &&
           Objects.equals(visibleSegments, that.visibleSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(knownPartitionChunks, standbySegments, visibleSegments);
  }

  @Override
  public String toString()
  {
    return "OvershadowableManagerPerTimeChunk{" +
           "knownPartitionChunks=" + knownPartitionChunks +
           ", standbySegments=" + standbySegments +
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
