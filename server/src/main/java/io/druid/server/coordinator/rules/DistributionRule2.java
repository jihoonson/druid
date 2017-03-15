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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.Objects;

public class DistributionRule2 implements Rule
{
  public static final String TYPE = "distributionRule";
  private static final EmittingLogger log = new EmittingLogger(DistributionRule2.class);
  private static final String assignedCount = "assignedCount";

//  private final List<String> smallDataSources;
  private final String bigDataSource;

  @JsonCreator
  public DistributionRule2(
//      @JsonProperty List<String> smallDataSources
      @JsonProperty String bigDataSource
  )
  {
//    this.smallDataSources = Objects.requireNonNull(smallDataSources);
    this.bigDataSource = Objects.requireNonNull(bigDataSource);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
//    return smallDataSources.contains(segment.getDataSource());
    return true;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return true;
  }

  @Override
  public CoordinatorStats run(
      DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment
  )
  {
    // Find servers which holds the segments of bigDataSource
    final List<ServerHolder> targetServerHolders = Lists.newArrayList();
    // TODO: consider storing servers by data sources
    for (ServerHolder eachHolder : params.getDruidCluster().getAllServers()) {
      if (eachHolder.getServer().getDataSource(bigDataSource) != null) {
        targetServerHolders.add(eachHolder);
      }
    }

    // The segment is replicated to the found servers
    final CoordinatorStats assignStats = assign(
        params.getReplicationManager(),
        params.getSegmentReplicantLookup()
              .getTotalReplicants(segment.getIdentifier()),
        0,
        targetServerHolders,
        segment
    );
    final CoordinatorStats stats = new CoordinatorStats();
    stats.accumulate(assignStats);

    return stats;
  }

  private CoordinatorStats assign(
      final ReplicationThrottler replicationManager,
      final int totalReplicantsInCluster,
      final int expectedReplicantsInTier,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(assignedCount, 0);

    int currTotalReplicantsInCluster = totalReplicantsInCluster;

    for (final ServerHolder holder : serverHolderList) {
      // TODO: check the server has enough space to store this segment
//      if (holder == null) {
//        log.warn(
//            "Not enough servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
//            segment.getIdentifier(),
//            expectedReplicantsInTier
//        );
//        break;
//      }

      if (segment.getSize() > holder.getAvailableSize()) {
        log.makeAlert(
            "Failed to replicate segment[%s] to server[%s] due to insufficient available space",
            segment.getIdentifier(),
            holder.getServer().getHost()
        )
           .addData("segmentSize", segment.getSize())
           .addData("availableSize", holder.getAvailableSize())
           .emit();
        continue; // TODO: continue? remove all replicated segments?
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
    }

    return stats;
  }
}
