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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.FirehoseV2;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class SplitIteratingFirehose<T, V extends SplitSource> implements FirehoseV2
{
  private final SplitReader splitReader;
  private final Iterator<T> objectIterator;
  private final Function<T, Iterator<V>> splitSourceFn;

  public SplitIteratingFirehose(ParseSpec parseSpec, Iterator<T> objectIterator, Function<T, Iterator<V>> splitSourceFn)
  {
    this.splitReader = parseSpec.createReader();
    this.objectIterator = objectIterator;
    this.splitSourceFn = splitSourceFn;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return new CloseableIterator<InputRow>()
    {
      Iterator<V> splitIterator = null;
      CloseableIterator<InputRow> rowIterator = null;

      @Override
      public boolean hasNext()
      {
        checkRowIterator();
        return rowIterator != null && rowIterator.hasNext();
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        return rowIterator.next();
      }

      private void checkRowIterator()
      {
        if (rowIterator == null || !rowIterator.hasNext()) {
          checkSplitIterator();
          if (splitIterator != null && splitIterator.hasNext()) {
            try {
              rowIterator = splitReader.read(splitIterator.next());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }

      private void checkSplitIterator()
      {
        if ((splitIterator == null || !splitIterator.hasNext()) && objectIterator.hasNext()) {
          splitIterator = splitSourceFn.apply(objectIterator.next());
        }
      }

      @Override
      public void close() throws IOException
      {
        if (rowIterator != null) {
          rowIterator.close();
        }
      }
    };
  }
}
