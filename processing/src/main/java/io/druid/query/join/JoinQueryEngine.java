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

package io.druid.query.join;

import com.google.common.base.Supplier;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.List;

public interface JoinQueryEngine
{
  Sequence<Row> process(
      final JoinQuery query,
      final Segment segment,
      final Supplier<Sequence<Row>> joinedBroadcastedSources,
      final StupidPool<ByteBuffer> pool
  );
}
