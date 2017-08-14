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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Iterators;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.epinephelinae.Grouper.Entry;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerde;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class AsyncAggregatingIterator<T> implements Iterator<Entry<T>>
{
  private final Iterator<Entry<T>> mergingIterator;
  private final ByteBuffer buffer;
  private final KeySerde<T> keySerde;
  private final AggregatorFactory[] aggregatorFactories;

  public AsyncAggregatingIterator(
      List<Iterator<Entry<T>>> iterators,
      Comparator<Entry<T>> comparator,
      ByteBuffer buffer,
      KeySerde<T> keySerde,
      AggregatorFactory[] aggregatorFactories,
      ExecutorService exec,
      int priority
  )
  {
    this.mergingIterator = Iterators.mergeSorted(iterators, comparator);
    this.buffer = buffer;
    this.keySerde = keySerde;
    this.aggregatorFactories = aggregatorFactories;

    exec.submit(
        new AbstractPrioritizedCallable<Void>(priority)
        {
          @Override
          public Void call() throws Exception
          {
            return null; // write to buffer
          }
        }
    );
  }

  @Override
  public boolean hasNext()
  {
    return false; // make next ready
  }

  @Override
  public Entry<T> next()
  {
    return null; // return and clear next
  }
}
