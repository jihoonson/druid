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

import org.apache.druid.data.input.InputRow;

import java.nio.channels.ByteChannel;
import java.util.Iterator;
import java.util.List;

public class CSVReader implements InputRowReader
{
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;

  public CSVReader(String listDelimiter, List<String> columns, boolean hasHeaderRow, int skipHeaderRows)
  {
    this.listDelimiter = listDelimiter;
    this.columns = columns;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;
  }

  @Override
  public Iterator<InputRow> read(ByteChannel inputChannel)
  {

    return null;
  }
}
