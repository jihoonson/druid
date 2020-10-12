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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV3.MergingDictionary;
import org.apache.druid.query.groupby.epinephelinae.MergedDictionary;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class DictionaryMergingQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final int ROWS_PER_SEGMENT = 100_000;
  private static final int NUM_SEGMENTS = 4;
  private static final GeneratorSchemaInfo SCHEMA_INFO = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

  private static List<QueryableIndex> INDEXES;
  private static List<DataSegment> SEGMENTS;
  private static Closer CLOSER;

  private ExecutorService service;

  @BeforeClass
  public static void setupClass()
  {
    CLOSER = Closer.create();

    INDEXES = new ArrayList<>();
    SEGMENTS = new ArrayList<>();

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      final DataSegment dataSegment = DataSegment.builder()
                                                 .dataSource("foo")
                                                 .interval(SCHEMA_INFO.getDataInterval())
                                                 .version("1")
                                                 .shardSpec(new NumberedShardSpec(i, NUM_SEGMENTS))
                                                 .size(0)
                                                 .build();
      SEGMENTS.add(dataSegment);

      final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());
      INDEXES.add(
          CLOSER.register(
              segmentGenerator.generate(dataSegment, SCHEMA_INFO, Granularities.HOUR, ROWS_PER_SEGMENT)
          )
      );
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException
  {
    CLOSER.close();
  }

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(4, "test-%d");
  }

  @After
  public void teardown()
  {
    service.shutdownNow();
  }

  @Test
  public void testMerge()
  {
    final DictionaryMergingQueryRunnerFactory factory = new DictionaryMergingQueryRunnerFactory();
    final SegmentIdMapper segmentIdMapper = new SegmentIdMapper();
    final List<QueryRunner<List<Iterator<DictionaryConversion>>>> runners = new ArrayList<>();
    for (int i = 0; i < SEGMENTS.size(); i++) {
      runners.add(
          QueryRunnerTestHelper.makeDictionaryScanRunner(
              new QueryableIndexSegment(INDEXES.get(i), SEGMENTS.get(i).getId()),
              "test",
              segmentIdMapper
          )
      );
    }
    final DictionaryMergingQueryRunner mergingRunner = factory.mergeRunners(
        service,
        runners
    );
    final DictionaryMergeQuery query = new DictionaryMergeQuery(
        new TableDataSource("foo"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(SCHEMA_INFO.getDataInterval())),
        ImmutableList.of(
//            new DefaultDimensionSpec("dimSequential", "dimSequential"),
//            new DefaultDimensionSpec("dimZipf", "dimZipf")
            new DefaultDimensionSpec("dimUniform", "dimUniform")
        )
    );
    final Sequence<List<Iterator<DictionaryConversion>>> sequence = mergingRunner.run(QueryPlus.wrap(query));
    final MergingDictionary[] merging = new MergingDictionary[query.getDimensions().size()];
    for (int i = 0; i < merging.length; i++) {
      merging[i] = new MergingDictionary(runners.size());
    }
    sequence.accumulate(
        merging,
        (accumulated, conversions) -> {
          assert accumulated.length == conversions.size();
          for (int i = 0; i < conversions.size(); i++) {
            while (conversions.get(i).hasNext()) {
              final DictionaryConversion conversion = conversions.get(i).next();
              accumulated[i].add(conversion.getVal(), conversion.getSegmentId(), conversion.getOldDictionaryId());
            }
          }
          return accumulated;
        }
    );

    for (MergingDictionary mergingDictionary : merging) {
      final MergedDictionary mergedDictionary = mergingDictionary.toImmutable();
      final List<String> sortedStrings = Arrays.asList(mergedDictionary.getDictionary());
      Collections.sort(sortedStrings);
      for (int i = 0; i < sortedStrings.size(); i++) {
        Assert.assertEquals(sortedStrings.get(i), mergedDictionary.lookup(i));
      }

      for (int i = 0; i < runners.size(); i++) {
        final int[] conversion = mergedDictionary.getDictionaryConversion(i);
        final int max = Arrays.stream(conversion).max().getAsInt();
        Assert.assertTrue(sortedStrings.size() > max);
      }
    }
  }
}
