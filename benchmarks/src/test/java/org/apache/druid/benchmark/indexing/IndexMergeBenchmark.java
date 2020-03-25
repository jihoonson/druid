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

package org.apache.druid.benchmark.indexing;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.gen.TestDataGenerator;
import org.apache.druid.data.gen.TestSchemaInfo;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexMergeBenchmark
{
  @Param({"5"})
  private int numSegments;

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  private static final Logger log = new Logger(IndexMergeBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    NullHandling.initializeForTests();
  }

  private List<QueryableIndex> indexesToMerge;
  private TestSchemaInfo schemaInfo;
  private File tmpDir;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
    JSON_MAPPER.setInjectableValues(injectableValues);
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    indexesToMerge = new ArrayList<>();

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    for (int i = 0; i < numSegments; i++) {
      TestDataGenerator gen = new TestDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }

      tmpDir = FileUtils.createTempDir();
      log.info("Using temp dir: " + tmpDir.getAbsolutePath());

      File indexFile = INDEX_MERGER_V9.persist(
          incIndex,
          tmpDir,
          new IndexSpec(),
          null
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      indexesToMerge.add(qIndex);
    }
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
            .withMetrics(schemaInfo.getAggsArray())
            .withRollup(rollup)
            .build()
        )
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void mergeV9(Blackhole blackhole) throws Exception
  {
    File tmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(), ".TEMPFILE");
    tmpFile.delete();
    tmpFile.mkdirs();
    try {
      log.info(tmpFile.getAbsolutePath() + " isFile: " + tmpFile.isFile() + " isDir:" + tmpFile.isDirectory());

      File mergedFile = INDEX_MERGER_V9.mergeQueryableIndex(
          indexesToMerge,
          rollup,
          schemaInfo.getAggsArray(),
          tmpFile,
          new IndexSpec(),
          null
      );

      blackhole.consume(mergedFile);
    }
    finally {
      tmpFile.delete();

    }

  }
}
