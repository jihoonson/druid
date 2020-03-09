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

package org.apache.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.jackson.DruidDefaultSerializersModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SequenceSerializationBenchmark
{
   @Param({"10000", "100000", "1000000"})
  private int numIntegers;

  private ObjectMapper smileMapper;
  private Sequence<Integer> sequence;

  @Setup
  public void setup()
  {
    smileMapper = new JacksonModule().smileMapper();
    smileMapper.registerModule(new DruidDefaultSerializersModule());

    sequence = new BaseSequence<>(
        new IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return IntStream.range(0, numIntegers).iterator();
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
          }
        }
    );
  }

  @Benchmark
  public void serializeYielder(Blackhole blackhole) throws IOException
  {
    Yielder<Integer> yielder = Yielders.each(sequence);
    ObjectWriter objectWriter = smileMapper.writer();
    try (BlackholeStream os = new BlackholeStream(blackhole)) {
      objectWriter.writeValue(os, yielder);
    }
  }

  @Benchmark
  public void serializeSequence(Blackhole blackhole) throws IOException
  {
    ObjectWriter objectWriter = smileMapper.writer();
    try (BlackholeStream os = new BlackholeStream(blackhole)) {
      objectWriter.writeValue(os, sequence);
    }
  }

  private static class BlackholeStream extends OutputStream
  {
    private final Blackhole blackhole;

    private BlackholeStream(Blackhole blackhole)
    {
      this.blackhole = blackhole;
    }

    @Override
    public void write(int b)
    {
      blackhole.consume(b);
    }

    @Override
    public void write(byte[] b)
    {
      blackhole.consume(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
      blackhole.consume(b);
    }
  }
}
