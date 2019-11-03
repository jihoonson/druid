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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.opencsv.CSVParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.java.util.common.parsers.Parsers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CSVReader extends TextReader
{
  private final CSVParser parser = new CSVParser();
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;
  private final Function<String, Object> multiValueFunction;
  @Nullable
  private List<String> columns;

  CSVReader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      String listDelimiter,
      @Nullable List<String> columns,
      boolean hasHeaderRow,
      int skipHeaderRows
  )
  {
    super(timestampSpec, dimensionsSpec);
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));
    this.columns = hasHeaderRow ? null : columns; // columns will be overriden by header row
  }

  @Override
  public InputRow readLine(String line) throws IOException
  {
    final String[] parsed = parser.parseLine(line);
    final Map<String, Object> zipped = Utils.zipMapPartial(
        Preconditions.checkNotNull(columns, "columns"),
        Iterables.transform(Arrays.asList(parsed), multiValueFunction)
    );
    return MapInputRowParser.parse(getTimestampSpec(), getDimensionsSpec(), zipped);
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
      setColumns(Arrays.asList(parser.parseLine(line)));
    }
    if (columns == null || columns.isEmpty()) {
      throw new ISE("Empty columns");
    }
  }

  private void setColumns(List<String> parsedLine)
  {
    columns = new ArrayList<>(parsedLine.size());
    for (int i = 0; i < parsedLine.size(); i++) {
      if (Strings.isNullOrEmpty(parsedLine.get(i))) {
        columns.add(ParserUtils.getDefaultColumnName(i));
      } else {
        columns.add(parsedLine.get(i));
      }
    }
    if (columns.isEmpty()) {
      columns = ParserUtils.generateFieldNames(parsedLine.size());
    }
    ParserUtils.validateFields(columns);
  }
}
