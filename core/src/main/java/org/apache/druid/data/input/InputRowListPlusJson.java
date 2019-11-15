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

package org.apache.druid.data.input;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class InputRowListPlusJson
{
  @Nullable
  private final List<InputRow> inputRows;

  /**
   * Can be null if it failed to parse to JSON.
   */
  @Nullable
  private final String rawJson;

  @Nullable
  private final ParseException parseException;

  public static InputRowListPlusJson of(@Nullable InputRow inputRow, @Nullable String rawJson)
  {
    return of(Collections.singletonList(inputRow), rawJson);
  }

  public static InputRowListPlusJson of(@Nullable List<InputRow> inputRows, @Nullable String rawJson)
  {
    return new InputRowListPlusJson(inputRows, rawJson, null);
  }

  public static InputRowListPlusJson of(@Nullable String rawJson, @Nullable ParseException parseException)
  {
    return new InputRowListPlusJson(null, rawJson, parseException);
  }

  private InputRowListPlusJson(
      @Nullable List<InputRow> inputRows,
      @Nullable String rawJson,
      @Nullable ParseException parseException
  )
  {
    Preconditions.checkArgument(
        (inputRows != null && !inputRows.isEmpty()) || rawJson != null || parseException != null,
        "One of inputRows, rawJson, or parseException must not be null"
    );
    this.inputRows = inputRows;
    this.rawJson = rawJson;
    this.parseException = parseException;
  }

  @Nullable
  public List<InputRow> getInputRows()
  {
    return inputRows;
  }

  @Nullable
  public String getRawJson()
  {
    return rawJson;
  }

  @Nullable
  public ParseException getParseException()
  {
    return parseException;
  }
}
