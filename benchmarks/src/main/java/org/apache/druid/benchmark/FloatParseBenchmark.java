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

import com.google.common.primitives.Floats;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class FloatParseBenchmark
{
  private static final int NUM_STRINGS = 100000;

  @Param({"0.1", "0.3", "0.5"})
  public double errorRatio;

  private List<String> floatStrings;

  @Setup(Level.Trial)
  public void setup()
  {
    if (floatStrings == null) {
      floatStrings = new ArrayList<>(NUM_STRINGS);
      final int numFloats = (int) (NUM_STRINGS * (1 - errorRatio));
      for (int i = 0; i < numFloats; i++) {
        final float f = ThreadLocalRandom.current().nextFloat() * ThreadLocalRandom.current().nextInt(NUM_STRINGS);
        floatStrings.add(String.valueOf(f));
      }
      final byte[] bytes = new byte[8];
      for (int i = 0; i < NUM_STRINGS - numFloats; i++) {
        ThreadLocalRandom.current().nextBytes(bytes);
        final String s = new String(bytes);
        floatStrings.add(s);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void tryParse(Blackhole blackhole)
  {
    for (String floatString : floatStrings) {
      blackhole.consume(Floats.tryParse(floatString));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void parseFloat(Blackhole blackhole)
  {
    for (String floatString : floatStrings) {
      try {
        blackhole.consume(Float.parseFloat(floatString));
      }
      catch (NumberFormatException e) {
        // do nothing
      }
    }
  }
}
