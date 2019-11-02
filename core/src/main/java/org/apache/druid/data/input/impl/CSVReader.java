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
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CSVReader extends TextReader
{
  private final CSVParser parser = new CSVParser();
  private final String listDelimiter; // TODO: use this
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;
  @Nullable
  private List<String> columns;

  CSVReader(
      TimestampSpec timestampSpec,
      String listDelimiter,
      @Nullable List<String> columns,
      boolean hasHeaderRow,
      int skipHeaderRows
  )
  {
    super(timestampSpec);
    this.listDelimiter = listDelimiter;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;
    this.columns = columns;

    if (hasHeaderRow && (columns != null && !columns.isEmpty())) {
      throw new IllegalArgumentException("columns must be empty if hasHeaderRow = true");
    }
  }

  @Override
  public InputRow readLine(String line) throws IOException
  {
    final String[] parsed = parser.parseLine(line);
    final Map<String, Object> zipped = Utils.zipMapPartial(columns, Arrays.asList(parsed));
    final DateTime timestamp = getTimestampSpec().extractTimestamp(zipped);
    if (timestamp == null) {
      throw new ParseException("null timestamp");
    }
    return new MapBasedInputRow(timestamp, columns, zipped);
  }

  @Override
  public int getNumHeaderLines()
  {
    return (hasHeaderRow ? 1 : 0) + skipHeaderRows;
  }

  @Override
  public void processHeaderLine(String line) throws IOException
  {
    if (hasHeaderRow && (columns == null || columns.isEmpty())) {
      columns = Arrays.asList(parser.parseLine(line));
    }
    if (columns == null || columns.isEmpty()) {
      throw new ISE("Empty columns");
    }
  }
}
