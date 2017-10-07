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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.DruidServer;
import io.druid.concurrent.Execs;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class CostBalancerStrategyDistributionBenchmark
{
  private static ListeningExecutorService balancerExec;
  private static List<DruidServer> servers = new ArrayList<>();
  private static List<TestLoadQueuePeon> loadQueuePeons = new ArrayList<>();
  private static BalancerStrategy<DruidServer> balancerStrategy;

  private static final String DATA_SOURCE_PREFIX = "dataSource_";
  private static final long startMillis = new DateTime("2017-01-01").getMillis();
  private static final long unitInterval = TimeUnit.DAYS.toMillis(1);
  private static final int numServers = 20;
  private static final int numDataSources = 1000;
  private static final int numSegments = 100019;

  private static final Map<Integer, Long> nextIntervals = new HashMap<>();

  @BeforeClass
  public static void setup()
  {
    balancerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(
        1,
        "coordinator-cost-balancer-%s"
    ));
    balancerStrategy = new CostBalancerStrategy<>(balancerExec);

    for (int i = 0; i < numServers; i++) {
      final DruidServer server = new DruidServer(
          "server_" + i,
          "host" + i,
          Long.MAX_VALUE,
          ServerType.HISTORICAL,
          "default",
          0
      );
      servers.add(server);
      loadQueuePeons.add(new TestLoadQueuePeon(server));
    }

    List<ServerHolder<DruidServer>> serverHolders = makeServerHolders();
    int i;
    for (i = 0; i < numDataSources; i++) {

      final long startInterval = startMillis;
      final long endInterval = startInterval + unitInterval;
      final DataSegment segment = newSegment(DATA_SOURCE_PREFIX + i, startInterval, endInterval);

      final ServerHolder serverHolder = balancerStrategy.findNewSegmentHomeReplicator(segment, serverHolders);

      serverHolder.getPeon().loadSegment(segment, null);
      nextIntervals.put(i, endInterval);
    }

    for (; i < numSegments; i++) {
      final int dataSourceIndex = ThreadLocalRandom.current().nextInt(numDataSources);
      final long startInterval = nextIntervals.get(dataSourceIndex);
      final long endInterval = startInterval + unitInterval;
      final DataSegment segment = newSegment(DATA_SOURCE_PREFIX + dataSourceIndex, startInterval, endInterval);

      final ServerHolder serverHolder = balancerStrategy.findNewSegmentHomeReplicator(segment, serverHolders);

      serverHolder.getPeon().loadSegment(segment, null);
      nextIntervals.put(dataSourceIndex, endInterval);
    }
  }

  @AfterClass
  public static void teardown()
  {
    balancerExec.shutdownNow();
  }

  private static DataSegment newSegment(String dataSource, long startInterval, long endInterval)
  {
    return new DataSegment(
        dataSource,
        new Interval(startInterval, endInterval),
        "0",
        null,
        ImmutableList.of("dim1"),
        ImmutableList.of("met1"),
        new NumberedShardSpec(0, 1),
        0,
        1
    );
  }

  private static List<ServerHolder<DruidServer>> makeServerHolders()
  {
    final List<ServerHolder<DruidServer>>serverHolders = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; i++) {
      serverHolders.add(
          new ServerHolder<>(servers.get(i), loadQueuePeons.get(i))
      );
    }
    return serverHolders;
  }

  @Test
  public void test()
  {
    final int checkPeriod = 100;
    final int maxSegmentsToMove = 5;
    final int iteration = 10000;

    printDistribution(0);

    final List<ServerHolder<DruidServer>> serverHolders = makeServerHolders();
    for (int iter = 0; iter < iteration; iter++) {

      for (int i = 0; i < maxSegmentsToMove; i++) {

        final BalancerSegmentHolder segmentToMove = balancerStrategy.pickSegmentToMove(serverHolders);
        if (segmentToMove != null) {
          final ServerHolder toServer = balancerStrategy.findNewSegmentHomeBalancer(
              segmentToMove.getSegment(),
              serverHolders
          );
          if (toServer != null) {
            final int serverIndex = Integer.valueOf(segmentToMove.getFromServer().getName().split("_")[1]);
            final DruidServer fromServer = servers.get(serverIndex);
            fromServer.removeDataSegment(segmentToMove.getSegment().getIdentifier());
            toServer.getPeon().loadSegment(segmentToMove.getSegment(), null);
          }
        }
      }
    }

    printDistribution(iteration);
  }

  private static void printDistribution(int iter)
  {
    final Map<DruidServer, Map<String, List<DataSegment>>> map = new HashMap<>();

    // distribution per druidServer
    // -> # of ds, # of segments
    int sumDsNum = 0;
    int sumSegNum = 0;
    double sumSquareDsNum = 0;
    double sumSquareSegNum = 0;
    for (DruidServer server : servers) {
      final Collection<DataSegment> segments = server.getSegments().values();
      sumDsNum += server.getDataSources().size();
      sumSegNum += segments.size();
      sumSquareDsNum += Math.pow(server.getDataSources().size(), 2);
      sumSquareSegNum += Math.pow(segments.size(), 2);

      final Map<String, List<DataSegment>> segmentMap = new HashMap<>(server.getDataSources().size());
      for (DataSegment segment : segments) {
        segmentMap.computeIfAbsent(segment.getDataSource(), k -> new ArrayList<>()).add(segment);
      }
      map.put(server, segmentMap);
    }

    double meanDsNum = (double) sumDsNum / servers.size();
    double meanSegNum = (double) sumSegNum / servers.size();
    double meanOfSquareSumDsNum = sumSquareDsNum / servers.size();
    double meanOfSquareSumSegNum = sumSquareSegNum / servers.size();
    double squareOfMeanDsNum = Math.pow(meanDsNum, 2);
    double squareOfMeanSegNum = Math.pow(meanSegNum, 2);

    System.out.println("======================== after " + iter + " iteration ======================");

    System.out.println("Distribution per server");
    System.out.println("DataSources");
    System.out.println("Mean: " + meanDsNum);
    System.out.println("Stdev: " + Math.sqrt(meanOfSquareSumDsNum - squareOfMeanDsNum));
    System.out.println();
    System.out.println("Segments");
    System.out.println("Mean: " + meanSegNum);
    System.out.println("Stdev: " + Math.sqrt(meanOfSquareSumSegNum - squareOfMeanSegNum));
    System.out.println();

    // distribution per dataSource
    // -> mean of # of segments per server
    // -> stdev of # of segments per server
    System.out.println("DataSource,Mean,Stdev,Max,Min");
    for (int i = 0; i < numDataSources; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;

      sumSegNum = 0;
      sumSquareSegNum = 0;
      double min = Double.POSITIVE_INFINITY, max = 0;

      for (DruidServer server : servers) {
        final Map<String, List<DataSegment>> segmentMap = map.get(server);
        if (segmentMap != null) {
          final List<DataSegment> segments = segmentMap.get(dataSource);
          if (segments != null) {
            sumSegNum += segments.size();
            sumSquareSegNum += Math.pow(segments.size(), 2);
            if (min > segments.size()) {
              min = segments.size();
            }
            if (max < segments.size()) {
              max = segments.size();
            }
          }
        }
      }

      double mean = sumSegNum / servers.size();
      double stdev = Math.sqrt(sumSquareSegNum / servers.size() - Math.pow(mean, 2));

      System.out.println(String.join(",", dataSource, String.valueOf(mean), String.valueOf(stdev), String.valueOf(max), String.valueOf(min)));
    }
  }

  private static class TestLoadQueuePeon extends LoadQueuePeon
  {
    private final DruidServer server;

    TestLoadQueuePeon(DruidServer server)
    {
      super(null, null, null, null, null, null);
      this.server = server;
    }

    @Override
    public void loadSegment(DataSegment segment, LoadPeonCallback callback)
    {
      server.addDataSegment(segment.getIdentifier(), segment);
    }

    @Override
    public void dropSegment(
        final DataSegment segment,
        final LoadPeonCallback callback
    )
    {
      server.removeDataSegment(segment.getIdentifier());
    }
  }
}
