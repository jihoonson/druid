/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.rules;

import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

public class BroadcastRule implements DistributionRule
{
  private static final EmittingLogger log = new EmittingLogger(BroadcastRule.class);
  private static final String TYPE = "broadcastRule";
  private static final String assignedCount = "assignedCount";

  // TODO: period
  // TODO: interval

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return false;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return false;
  }

  @Override
  public CoordinatorStats run(
      DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    final Set<DataSegment> availableSegments = params.getAvailableSegments();

//    boolean needDrop = true;

    int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());

    final Set<ServerHolder> allServers = params.getDruidCluster().getAllServers();
    final int expectedReplicantsInTier = allServers.size();
    final int totalReplicantsInTier = params.getSegmentReplicantLookup()
                                            .getTotalReplicants(segment.getIdentifier());
//    final int loadedReplicantsInTier = params.getSegmentReplicantLookup()
//                                             .getLoadedReplicants(segment.getIdentifier());

    if (allServers.size() == 0) {
      log.makeAlert("There is no servers! Check your cluster configuration!").emit();
    } else {
      final List<ServerHolder> serverHolderList = Lists.newArrayList(allServers);
      if (availableSegments.contains(segment)) {
        CoordinatorStats assignStats = assign(
            params.getReplicationManager(),
            totalReplicantsInCluster,
            expectedReplicantsInTier,
            totalReplicantsInTier,
            serverHolderList,
            segment
        );
        stats.accumulate(assignStats);
        totalReplicantsInCluster += assignStats.getGlobalStats().get(assignedCount).get();
      }

//      loadStatus.put(tier, expectedReplicantsInTier - loadedReplicantsInTier);
//      needDrop = expectedReplicantsInTier < loadedReplicantsInTier;
    }

    // TODO: Remove over-replication?
//    if (needDrop) {
//      stats.accumulate(drop(segment, params));
//    }

    return stats;
  }

  private CoordinatorStats assign(
      final ReplicationThrottler replicationManager,
      final int totalReplicantsInCluster,
      final int expectedReplicantsInTier,
      final int totalReplicantsInTier,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(assignedCount, 0);

    int currReplicantsInTier = totalReplicantsInTier;
    int currTotalReplicantsInCluster = totalReplicantsInCluster;

    for (final ServerHolder holder : serverHolderList) {
      if (holder == null) {
        log.warn(
            "Not enough servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
            segment.getIdentifier(),
            expectedReplicantsInTier
        );
        break;
      }

      // TODO: ?
      if (currTotalReplicantsInCluster > 0) {
        replicationManager.registerReplicantCreation(
            null, segment.getIdentifier(), holder.getServer().getHost()
        );
      }

      holder.getPeon().loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              replicationManager.unregisterReplicantCreation(
                  null,
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      stats.addToGlobalStat(assignedCount, 1);
      ++currReplicantsInTier;
      ++currTotalReplicantsInCluster;
    }

    return stats;
  }

//  private CoordinatorStats drop(
////      final Map<String, Integer> loadStatus,
//      final DataSegment segment,
//      final DruidCoordinatorRuntimeParams params
//  )
//  {
//    CoordinatorStats stats = new CoordinatorStats();
//
//    final ReplicationThrottler replicationManager = params.getReplicationManager();
//
//    // Find all instances of this segment across tiers
//    Map<String, Integer> replicantsByTier = params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier());
//
//    for (Map.Entry<String, Integer> entry : replicantsByTier.entrySet()) {
//      final String tier = entry.getKey();
//      int loadedNumReplicantsForTier = entry.getValue();
//      int expectedNumReplicantsForTier = getNumReplicants(tier);
//
//      stats.addToTieredStat(droppedCount, tier, 0);
//
//      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().get(tier);
//      if (serverQueue == null) {
//        log.makeAlert("No holders found for tier[%s]", entry.getKey()).emit();
//        continue;
//      }
//
//      List<ServerHolder> droppedServers = Lists.newArrayList();
//      while (loadedNumReplicantsForTier > expectedNumReplicantsForTier) {
//        final ServerHolder holder = serverQueue.pollLast();
//        if (holder == null) {
//          log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getIdentifier());
//          break;
//        }
//
//        if (holder.isServingSegment(segment)) {
//          holder.getPeon().dropSegment(
//              segment,
//              null
//          );
//          --loadedNumReplicantsForTier;
//          stats.addToTieredStat(droppedCount, tier, 1);
//        }
//        droppedServers.add(holder);
//      }
//      serverQueue.addAll(droppedServers);
//    }
//
//    return stats;
//  }
}
