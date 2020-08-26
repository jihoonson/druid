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

package org.apache.druid.segment.incremental;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CircularBuffer;

import javax.annotation.Nullable;

public class ParseExceptionHandler
{
  private static final Logger LOG = new Logger(ParseExceptionHandler.class);

  private final RowIngestionMeters rowIngestionMeters;
  private final boolean logParseExceptions;
  private final int maxAllowedParseExceptions;
  @Nullable
  private final CircularBuffer<Throwable> savedParseExceptions;

  public ParseExceptionHandler(
      RowIngestionMeters rowIngestionMeters,
      boolean logParseExceptions,
      int maxAllowedParseExceptions,
      int maxSavedParseExceptions
  )
  {
    this.rowIngestionMeters = rowIngestionMeters;
    this.logParseExceptions = logParseExceptions;
    this.maxAllowedParseExceptions = maxAllowedParseExceptions;
    if (maxSavedParseExceptions > 0) {
      this.savedParseExceptions = new CircularBuffer<>(maxSavedParseExceptions);
    } else {
      this.savedParseExceptions = null;
    }
  }

  public void handle(@Nullable ParseException e)
  {
    if (e == null) {
      return;
    }
    rowIngestionMeters.incrementUnparseable();

    if (logParseExceptions) {
      LOG.error(e, "Encountered parse exception");
    }

    if (rowIngestionMeters.getUnparseable() > maxAllowedParseExceptions) {
      throw new RuntimeException("Max allowed parse exceptions exceeded");
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(e);
    }
  }
}
