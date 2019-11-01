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
import java.util.stream.Stream;

public class SplitIteratingFirehose<T> implements FirehoseV2
{
  private final SplitReader splitReader;
  private final Iterator<SplitSource<T>> sourceIterator;

  public SplitIteratingFirehose(
      TimestampSpec timestampSpec,
      InputFormat inputFormat,
      Stream<SplitSource<T>> sourceStream
  )
  {
    this.splitReader = inputFormat.createReader(timestampSpec);
    this.sourceIterator = sourceStream.iterator();
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return new CloseableIterator<InputRow>()
    {
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
          if (sourceIterator.hasNext()) {
            try {
              rowIterator = splitReader.read(sourceIterator.next());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
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
