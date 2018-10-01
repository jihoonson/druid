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

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.guava.ParallelMergeCombineSequence2;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ParallelResultMergeQueryRunner<T> implements QueryRunner<T>
{
  private final ExecutorService exec;
  private final int batchSize;
  private final List<QueryRunner<T>> runners;
  private final Ordering<T> ordering;
  private final BinaryFn<T, T, T> mergeFn;

  public ParallelResultMergeQueryRunner(
      ExecutorService exec,
      int batchSize,
      List<QueryRunner<T>> runners,
      Ordering<T> ordering,
      BinaryFn<T, T, T> mergeFn
  )
  {
    this.exec = exec;
    this.batchSize = batchSize;
    this.runners = runners;
    this.ordering = ordering;
    this.mergeFn = mergeFn;
  }

  @Override
  public Sequence<T> run(
      QueryPlus<T> queryPlus, Map<String, Object> responseContext
  )
  {
    return new ParallelMergeCombineSequence2<>(
        exec,
        runners.stream().map(runner -> runner.run(queryPlus, responseContext)).collect(Collectors.toList()),
        ordering,
        mergeFn,
        batchSize
    );
  }
}
