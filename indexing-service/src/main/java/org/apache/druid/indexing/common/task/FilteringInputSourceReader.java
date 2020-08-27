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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.incremental.RowIngestionMeters;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class FilteringInputSourceReader
{
  private final InputSourceReader baseReader;
  private final RowIngestionMeters rowIngestionMeters;

  public FilteringInputSourceReader(
      InputSourceReader baseReader,
      RowIngestionMeters rowIngestionMeters
  )
  {
    this.baseReader = baseReader;
    this.rowIngestionMeters = rowIngestionMeters;
  }

  public CloseableIterator<InputRow> read(Predicate<InputRow> filter) throws IOException
  {
    final CloseableIterator<InputRow> delegate = baseReader.read();
    return new CloseableIterator<InputRow>()
    {
      InputRow next;

      @Override
      public boolean hasNext()
      {
        while (next == null && delegate.hasNext()) {
          final InputRow row = delegate.next();
          if (filter.test(row)) {
            next = row;
          } else {
            rowIngestionMeters.incrementThrownAway();
          }
        }
        return next != null;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final InputRow row = next;
        next = null;
        return row;
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }
}
