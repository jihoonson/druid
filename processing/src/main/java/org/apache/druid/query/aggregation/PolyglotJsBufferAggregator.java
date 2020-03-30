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

package org.apache.druid.query.aggregation;

import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class PolyglotJsBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector[] selectorList;
  private final Context context;
  private final Value resetFn;
  private final Value aggFn;
  private final Object[] args;

  public PolyglotJsBufferAggregator(
      List<BaseObjectColumnValueSelector> selectorList,
      String resetFn,
      String aggFn
  )
  {
    this.selectorList = selectorList.toArray(new BaseObjectColumnValueSelector[0]);
    context = Context.create();
    this.resetFn = context.eval("js", resetFn);
    this.aggFn = context.eval("js", aggFn);
    this.args = new Object[selectorList.size() + 1];
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, resetFn.execute().asDouble());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    args[0] = buf.getDouble(position);
    for (int i = 0; i < selectorList.length; i++) {
      args[i + 1] = selectorList[i].getObject();
    }
    buf.putDouble(position, aggFn.execute(args).asDouble());
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    context.close();
  }
}
