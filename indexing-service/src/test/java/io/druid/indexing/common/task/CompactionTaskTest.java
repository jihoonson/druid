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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.segment.IndexIO;
import io.druid.segment.column.ColumnConfig;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CompactionTaskTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final CompactionTask task = new CompactionTask(
        null,
        null,
        "testSource",
        Intervals.of("2016-12-21/2017-10-15"),
        new IndexTuningConfig(
            null,
            10,
            20,
            null,
            40,
            null,
            50,
            true,
            false,
            false,
            false,
            100L
        ),
        ImmutableMap.of("testKey", "testVal"),
        GuiceInjectors.makeStartupInjector(),
        new IndexIO(jsonMapper, () -> 0),
        jsonMapper
    );
    final String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task);
    System.out.println(json);
    Assert.assertEquals(task, jsonMapper.readValue(json, CompactionTask.class));
  }

  @Test
  public void testCompactionTask() throws Exception
  {
    final File tmpFile = File.createTempFile("druid", "index", TaskTestBase.BASE_DIR);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,d,val\n");
      writer.write("unparseable,a,1\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-02T00:00:10Z,a,1\n");
      writer.write("2014-01-03T00:00:10Z,a,1\n");
    }

    // GranularitySpec.intervals and numShards must be null to verify reportParseException=false is respected both in
    // IndexTask.determineShardSpecs() and IndexTask.generateAndPublishSegments()
    final IndexIngestionSpec parseExceptionIgnoreSpec = TaskTestBase.createIngestionSpec(
        TaskTestBase.BASE_DIR,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                null,
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "dim", "val"),
            true,
            0
        ),
        null,
        TaskTestBase.createTuningConfig(2, null, null, null, false, false, false), // ignore parse exception,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null
    );

    final List<DataSegment> segments = TaskTestBase.runTask(indexTask);

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());

    CompactionTask compactionTask = new CompactionTask(
        null,
        null,
        "test",
        new Interval("2014-01-01/2014-02-01"),
        null,
        null,
        GuiceInjectors.makeStartupInjector(),
        new IndexIO(new DefaultObjectMapper(), new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }),
        new DefaultObjectMapper()
    );

    TaskTestBase.runTask(compactionTask);
  }

}
