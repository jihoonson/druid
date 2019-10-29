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

import com.opencsv.CSVParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class CSVReader implements SplitReader
{
  private final CSVParser parser = new CSVParser();
  private final TimestampSpec timestampSpec;
  private final String listDelimiter; // TODO: use this
  @Nullable
  private final List<String> columns;
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;

  CSVReader(
      TimestampSpec timestampSpec,
      String listDelimiter,
      @Nullable List<String> columns,
      boolean hasHeaderRow,
      int skipHeaderRows
  )
  {
    this.timestampSpec = timestampSpec;
    this.listDelimiter = listDelimiter;
    this.columns = columns;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;
  }

  @Override
  public CloseableIterator<InputRow> read(SplitSource splitSource) throws IOException
  {
    final BufferedReader reader;
    reader = new BufferedReader(new InputStreamReader(splitSource.open(), StringUtils.UTF8_STRING));
    final List<String> columns;
    long headerBytes = 0;
    if (hasHeaderRow) {
      final String headerLine = reader.readLine();
      headerBytes += headerLine.length();
      columns = Arrays.asList(parser.parseLine(headerLine));
    } else {
      columns = this.columns;
    }
    if (columns == null || columns.isEmpty()) {
      throw new ISE("Empty columns");
    }

    for (int i = 0; i < skipHeaderRows; i++) {
      headerBytes += reader.readLine().length();
    }
    final long skippedBytes = headerBytes;

    return new CloseableIterator<InputRow>()
    {
      String nextLine = null;
      long readBytes = skippedBytes;

      @Override
      public boolean hasNext()
      {
        if (nextLine != null) {
          return true;
        } else {
          try {
            if (readBytes < splitSource.getSplit().length()) {
              nextLine = reader.readLine();
              if (nextLine != null) {
                readBytes += nextLine.length();
              }
              return (nextLine != null);
            } else {
              return false;
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public InputRow next()
      {
        if (nextLine != null || hasNext()) {
          String line = nextLine;
          nextLine = null;
          try {
            final String[] parsed = parser.parseLine(line);
            final Map<String, Object> zipped = Utils.zipMapPartial(columns, Arrays.asList(parsed));
            final DateTime timestamp = timestampSpec.extractTimestamp(zipped);
            if (timestamp == null) {
              throw new ParseException("null timestamp");
            }
            return new MapBasedInputRow(timestamp, columns, zipped);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }
    };
  }
}
