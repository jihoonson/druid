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
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: bulk build

/**
 * Not thread-safe
 * @param <T>
 */
public class SameVersionPartitionChunkManager<T extends Overshadowable<T>>
{
  private enum State
  {
    STANDBY,
    VISIBLE,
    OVERSHADOWED
  }

  private final Map<Integer, PartitionChunkWithAvailability> knownPartitionChunks; // served segments

  private final Map<Integer, PartitionChunkWithAvailability> standbySegments;
  private final Map<Integer, PartitionChunkWithAvailability> visibleSegments;
  private final Map<Integer, PartitionChunkWithAvailability> overshadowedSegments;

  public SameVersionPartitionChunkManager()
  {
    this.knownPartitionChunks = new HashMap<>();
    this.standbySegments = new HashMap<>();
    this.visibleSegments = new HashMap<>();
    this.overshadowedSegments = new HashMap<>();
  }

  public SameVersionPartitionChunkManager(SameVersionPartitionChunkManager<T> other)
  {
    this.knownPartitionChunks = new HashMap<>(other.knownPartitionChunks);
    this.standbySegments = new HashMap<>(other.standbySegments);
    this.visibleSegments = new HashMap<>(other.visibleSegments);
    this.overshadowedSegments = new HashMap<>(other.overshadowedSegments);
  }

  private Map<Integer, PartitionChunkWithAvailability> getSegmentMapOf(State state)
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
    final PartitionChunkWithAvailability partitionChunk = getSegmentMapOf(from).get(partitionChunkNumber);
    transitPartitionChunkState(partitionChunk, from, to);
  }

  private void transitPartitionChunkState(PartitionChunkWithAvailability partitionChunk, State from, State to)
  {
    Preconditions.checkNotNull(partitionChunk, "partitionChunk");
    final Map<Integer, PartitionChunkWithAvailability> fromMap = getSegmentMapOf(from);
    final Map<Integer, PartitionChunkWithAvailability> toMap = getSegmentMapOf(to);
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

  private Stream<PartitionChunkWithAvailability> partitionChunksOf(Set<Integer> partitionIds)
  {
    return partitionIds.stream().map(knownPartitionChunks::get).filter(Objects::nonNull);
  }

  public void add(PartitionChunk<T> chunk)
  {
    add(chunk, true);
  }

  public void add(PartitionChunk<T> chunk, boolean available)
  {
    final PartitionChunkWithAvailability existingChunk = knownPartitionChunks.get(chunk.getChunkNumber());
    final PartitionChunkWithAvailability chunkWithAvailability;
    if (existingChunk == null) {
      chunkWithAvailability = new PartitionChunkWithAvailability(chunk, available);
    } else {
      if (!existingChunk.chunk.equals(chunk)) {
        throw new ISE(
            "existingChunk[%s] is different from newChunk[%s] for partitionId[%d]",
            existingChunk,
            chunk,
            chunk.getChunkNumber()
        );
      }

      existingChunk.setAvailable(available);
      chunkWithAvailability = existingChunk;
    }

    // Decide an initial state of the given chunk.
    final boolean overshadowed = knownPartitionChunks
        .values()
        .stream()
        .anyMatch(knownChunk -> knownChunk.getObject().getOvershadowedGroup().contains(knownChunk.getChunkNumber()));

    if (overshadowed) {
      overshadowedSegments.put(chunk.getChunkNumber(), chunkWithAvailability);
    } else {
      // Check all segments in the atomicUpdateGroup have the same overshadowedGroup.
      final Set<Integer> overshadowedGroup = chunk.getObject().getOvershadowedGroup();
      if (!overshadowedGroup.isEmpty()) {
        final Set<Integer> atomicUpdateGroup = chunk.getObject().getAtomicUpdateGroup();

        if (!atomicUpdateGroup.isEmpty()) {

          final boolean allAtomicUpdateGroupHaveSameOvershadowedGroup = partitionChunksOf(atomicUpdateGroup)
              .allMatch(eachAug -> overshadowedGroup.equals(eachAug.getObject().getOvershadowedGroup()));

          if (!allAtomicUpdateGroupHaveSameOvershadowedGroup) {
            throw new ISE("all partitions of the same atomicUpdateGroup should have the same overshadowedGroup");
          }
        }
      }

      addToStandby(chunkWithAvailability);
    }

    chunk.getObject().getOvershadowedGroup()
         .stream()
         .map(visibleSegments::get)
         .filter(Objects::nonNull)
         .forEach(visibleChunk -> transitPartitionChunkState(visibleChunk, State.VISIBLE, State.OVERSHADOWED));
    chunk.getObject().getOvershadowedGroup()
         .stream()
         .map(standbySegments::get)
         .filter(Objects::nonNull)
         .forEach(standbyChunk -> transitPartitionChunkState(standbyChunk, State.STANDBY , State.OVERSHADOWED));
  }

  private void tryStandbyToVisible(PartitionChunkWithAvailability standbyChunk)
  {
    if (visibleSegments.isEmpty()) {
      // Since the input segment is not overshadowed by any, it should be visible if there's no visible segment
      transitPartitionChunkState(standbyChunk, State.STANDBY, State.VISIBLE);
    } else {
      final Set<Integer> atomicUpdateGroupIds = standbyChunk.getObject().getAtomicUpdateGroup();

      // if the entire atomicUpdateGroup are standby, move them to visibleSegments.
      final List<PartitionChunkWithAvailability> standbyAtomicUpdateGroup;
      if (atomicUpdateGroupIds.isEmpty()) {
        // atomicUpdateGroup is a singleton list of the segment itself if it's empty
        standbyAtomicUpdateGroup = Collections.singletonList(standbyChunk);
      } else {
        standbyAtomicUpdateGroup = atomicUpdateGroupIds.stream().map(standbySegments::get).collect(Collectors.toList());
      }

      final Set<Integer> standbyAtomicUpdateGroupIds = standbyAtomicUpdateGroup
          .stream()
          .map(PartitionChunkWithAvailability::getChunkNumber)
          .collect(Collectors.toSet());

      if (atomicUpdateGroupIds.equals(standbyAtomicUpdateGroupIds)) {
        standbyAtomicUpdateGroup.forEach(chunk -> transitPartitionChunkState(chunk, State.STANDBY, State.VISIBLE));
      }
    }
  }

  private void addToStandby(PartitionChunkWithAvailability chunk)
  {
    standbySegments.put(chunk.getChunkNumber(), chunk);
    tryStandbyToVisible(chunk);
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> partitionChunk)
  {
    return remove(partitionChunk, true);
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> partitionChunk, boolean keepReference)
  {
    final PartitionChunkWithAvailability knownChunk = knownPartitionChunks.get(partitionChunk.getChunkNumber());
    if (knownChunk == null) {
      return null;
    }

    if (!knownChunk.chunk.equals(partitionChunk)) {
      throw new ISE(
          "WTH? Same partitionId[%d], but known partition[%s] is different from the input partition[%s]",
          partitionChunk.getChunkNumber(),
          knownChunk,
          partitionChunk
      );
    }

    if (visibleSegments.containsKey(partitionChunk.getChunkNumber())) {
      tryVisibleToStandby(knownChunk, keepReference);
    } else if (standbySegments.containsKey(partitionChunk.getChunkNumber())) {
      standbySegments.remove(partitionChunk.getChunkNumber());
    } else if (overshadowedSegments.containsKey(partitionChunk.getChunkNumber())) {
      overshadowedSegments.remove(partitionChunk.getChunkNumber());
    } else {
      throw new ISE("Unknown status of partition[%s]", partitionChunk);
    }

    if (keepReference) {
      knownChunk.setAvailable(false);
      return partitionChunk;
    } else {
      return knownPartitionChunks.remove(partitionChunk.getChunkNumber()).chunk;
    }
  }

  private void tryVisibleToStandby(PartitionChunkWithAvailability removedChunk, boolean keepReference)
  {
    if (!visibleSegments.containsKey(removedChunk.getChunkNumber())) {
      throw new ISE("partition[%s] is not in visible state", removedChunk);
    }

    final T removedObject = removedChunk.getObject();

    // Since a visible chunk is removed, the state of the atomicUpdateGroup of the removed chunk should be changed to
    // standby if there's any completely available atomic update group overshadowed by the removed chunk. Also, the
    // latest atomic update group overshadowed by the removed chunk should be visible.
    final Set<Integer> overshadowedPartitionIds = removedObject.getOvershadowedGroup();
    final List<PartitionChunkWithAvailability> onlineSegmentsInOvershadowedGroup = findOvershadowedChunks(
        overshadowedPartitionIds
    );

    if (!onlineSegmentsInOvershadowedGroup.isEmpty()) {
      final List<PartitionChunkWithAvailability> latestAvailableAtomicUpdateGroup = findLatestOnlineAtomicUpdateGroup(
          onlineSegmentsInOvershadowedGroup
      );

      // sanity check
      Preconditions.checkState(
          latestAvailableAtomicUpdateGroup.stream().allMatch(candidate -> candidate.available),
          "WTH? Entire latestOnlineAtomicUpdateGroup must be available"
      );

      if (!latestAvailableAtomicUpdateGroup.isEmpty()) {
        // if found, replace the current atomicUpdateGroup with the latestOnlineAtomicUpdateGroup
        removedObject.getNonEmptyAtomicUpdateGroup(removedChunk.getChunkNumber())
                     .forEach(partitionId -> transitPartitionChunkState(partitionId, State.VISIBLE, State.STANDBY));
        latestAvailableAtomicUpdateGroup.forEach(
            eachChunk -> transitPartitionChunkState(eachChunk, State.OVERSHADOWED, State.VISIBLE)
        );
      } else {
        if (!keepReference) {
          visibleSegments.remove(removedChunk.getChunkNumber());
        }

        // If all atomicUpdateGroup of the removed chunk are not visible, we should fall back to the previous one.
        final List<PartitionChunkWithAvailability> visibleAtomicUpdateGroupOfRemovedChunk = removedChunk
            .getAtomicUpdateGroup()
            .stream()
            .map(visibleSegments::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        if (visibleAtomicUpdateGroupOfRemovedChunk.isEmpty()
            || visibleAtomicUpdateGroupOfRemovedChunk.stream().noneMatch(chunk -> chunk.available)) {
          removedChunk.getOvershadowedGroup()
                      .forEach(eachChunk -> transitPartitionChunkState(eachChunk, State.OVERSHADOWED, State.VISIBLE));
        }
      }
    }
  }

  // TODO: improve this: looks like the first if is unnecessary sometimes
  private List<PartitionChunkWithAvailability> findLatestOnlineAtomicUpdateGroup(List<PartitionChunkWithAvailability> chunks)
  {
    if (chunks.stream().allMatch(chunk -> chunk.available)) {
      return chunks;
    } else {
      final Set<Integer> allOvershadowedGroupOfChunks = chunks.stream()
                                                              .flatMap(chunk -> chunk.getOvershadowedGroup().stream())
                                                              .collect(Collectors.toSet());

      if (allOvershadowedGroupOfChunks.isEmpty()) {
        return Collections.emptyList();
      } else {
        return findLatestOnlineAtomicUpdateGroup(findOvershadowedChunks(allOvershadowedGroupOfChunks));
      }
    }
  }

  /**
   * Finds _available_ chunks corresponding to the given partitionIds from overshadowedSegments.
   * Returns an empty list if all of them are not in overshadowedSegments. This can happen if the segment graph in
   * knownPartitionChunks is incomplete, e.g., as in historicals.
   */
  private List<PartitionChunkWithAvailability> findOvershadowedChunks(Set<Integer> partitionIds)
  {
//    final Map<Integer, PartitionChunkWithAvailability> candidates = buildAllOnlineCandidates();
    final List<PartitionChunkWithAvailability> found = partitionIds.stream()
                                                                   .map(overshadowedSegments::get)
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
    return visibleSegments.get(partitionId).chunk;
  }

  public SortedSet<PartitionChunk<T>> getVisibles()
  {
    return visibleSegments.values()
                          .stream()
                          .map(each -> each.chunk)
                          .collect(Collectors.toCollection(TreeSet::new));
  }

  public Collection<PartitionChunk<T>> getOvershadowed()
  {
    return overshadowedSegments.values().stream().map(each -> each.chunk).collect(Collectors.toList());
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
    SameVersionPartitionChunkManager<?> that = (SameVersionPartitionChunkManager<?>) o;
    return Objects.equals(knownPartitionChunks, that.knownPartitionChunks) &&
           Objects.equals(standbySegments, that.standbySegments) &&
           Objects.equals(visibleSegments, that.visibleSegments) &&
           Objects.equals(overshadowedSegments, that.overshadowedSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(knownPartitionChunks, standbySegments, visibleSegments, overshadowedSegments);
  }

  @Override
  public String toString()
  {
    return "SameVersionPartitionChunkManager{" +
           "knownPartitionChunks=" + knownPartitionChunks +
           ", standbySegments=" + standbySegments +
           ", visibleSegments=" + visibleSegments +
           ", overshadowedSegments=" + overshadowedSegments +
           '}';
  }

  private class PartitionChunkWithAvailability
  {
    private final PartitionChunk<T> chunk;
    private boolean available; // indicate this segment is available in any server

    private PartitionChunkWithAvailability(PartitionChunk<T> chunk, boolean available)
    {
      this.chunk = chunk;
      this.available = available;
    }

    private void setAvailable(boolean available)
    {
      this.available = available;
    }

    private int getChunkNumber()
    {
      return chunk.getChunkNumber();
    }

    private T getObject()
    {
      return chunk.getObject();
    }

    private Set<Integer> getOvershadowedGroup()
    {
      return chunk.getObject().getOvershadowedGroup();
    }

    private Set<Integer> getAtomicUpdateGroup()
    {
      return chunk.getObject().getAtomicUpdateGroup();
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
      PartitionChunkWithAvailability that = (PartitionChunkWithAvailability) o;
      return available == that.available &&
             Objects.equals(chunk, that.chunk);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(chunk, available);
    }

    @Override
    public String toString()
    {
      return "PartitionChunkWithAvailability{" +
             "chunk=" + chunk +
             ", available=" + available +
             '}';
    }
  }
}
