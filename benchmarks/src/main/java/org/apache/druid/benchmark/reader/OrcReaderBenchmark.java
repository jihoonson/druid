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

package org.apache.druid.benchmark.reader;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.orc.OrcReader;
import org.apache.druid.firehose.s3.S3Source;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.hadoop.conf.Configuration;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class OrcReaderBenchmark
{
  private final URI uri = URI.create("s3a://imply-example-data/jihoon/lineitem-apache.orc");

  @Param({"true", "false"})
  private boolean fetch;

  private File tempDir;
  private ServerSideEncryptingAmazonS3 amazonS3;
  private Configuration conf;
  private ExecutorService executorService;

  @Setup
  public void setup()
  {
    final String accessKey = "AKIAJEWDJE2GCIXRDP4A";
    final String secretKey = "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";
    final AmazonS3ClientBuilder builder = AmazonS3Client
        .builder()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
    amazonS3 = new ServerSideEncryptingAmazonS3(
        builder.build(),
        new NoopServerSideEncryption()
    );
    conf = new Configuration();
    conf.set("fs.s3a.access.key", accessKey);
    conf.set("fs.s3a.secret.key", secretKey);

    tempDir = Files.createTempDir();
    executorService = Execs.multiThreaded(4, "orc-reader-benchmark-%d");
  }

  @TearDown
  public void tearDown()
  {
    tempDir.delete();
    executorService.shutdownNow();
  }

  private OrcReader createReader()
  {
    return new OrcReader(
        conf,
        new InputRowSchema(
            new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "l_orderkey",
                        "l_partkey",
                        "l_suppkey",
                        "l_linenumber",
                        "l_returnflag",
                        "l_linestatus",
                        "l_shipdate",
                        "l_commitdate",
                        "l_receiptdate",
                        "l_shipinstruct",
                        "l_shipmode",
                        "l_comment"
                    )
                )
            ),
            Collections.emptyList()
        ),
        new JSONPathSpec(true, null),
        fetch
    );
  }

  @Benchmark
  public void readFully(Blackhole blackhole) throws IOException
  {
    final OrcReader reader = createReader();
    try (CloseableIterator<InputRow> iterator = reader.read(new S3Source(amazonS3, uri), tempDir)) {
      while (iterator.hasNext()) {
        blackhole.consume(iterator.next());
      }
    }
  }

  @Benchmark
  public void readParallel(Blackhole blackhole) throws ExecutionException, InterruptedException
  {
    final List<Future> futures = new ArrayList<>(2);
    futures.add(
        executorService.submit(() -> {
          final OrcReader reader = createReader();
          try (CloseableIterator<InputRow> iterator = reader.read(new S3Source(amazonS3, uri, 0, 83_886_080), tempDir)) {
            while (iterator.hasNext()) {
              blackhole.consume(iterator.next());
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
    );
    futures.add(
        executorService.submit(() -> {
          final OrcReader reader = createReader();
          try (CloseableIterator<InputRow> iterator = reader.read(new S3Source(amazonS3, uri, 83_886_080, Long.MAX_VALUE), tempDir)) {
            while (iterator.hasNext()) {
              blackhole.consume(iterator.next());
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
    );
    for (Future future : futures) {
      future.get();
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(OrcReaderBenchmark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
