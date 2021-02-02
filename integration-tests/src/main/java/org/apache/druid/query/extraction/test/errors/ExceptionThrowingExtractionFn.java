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

package org.apache.druid.query.extraction.test.errors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.extraction.ExtractionFn;

import javax.annotation.Nullable;

public class ExceptionThrowingExtractionFn implements ExtractionFn
{
  private static final String ERROR_MESSAGE = "Query error code test";

  private final RuntimeException exception;

  @JsonCreator
  public ExceptionThrowingExtractionFn(
      @JsonProperty("errorCode") String errorCode
  )
  {
    Preconditions.checkNotNull(errorCode, "errorCode");
    switch (errorCode) {
      case QueryCapacityExceededException.ERROR_CODE:
        this.exception = QueryCapacityExceededException.withErrorMessageAndResolvedHost(ERROR_MESSAGE);
        break;
      case ResourceLimitExceededException.ERROR_CODE:
        this.exception = new ResourceLimitExceededException(ERROR_MESSAGE);
        break;
      case QueryUnsupportedException.ERROR_CODE:
        this.exception = new QueryUnsupportedException(ERROR_MESSAGE);
        break;
      default:
        throw new IAE("Unknown error code [%s]", errorCode);
    }
  }

  @Nullable
  @Override
  public String apply(@Nullable Object value)
  {
    throw exception;
  }

  @Nullable
  @Override
  public String apply(@Nullable String value)
  {
    throw exception;
  }

  @Override
  public String apply(long value)
  {
    throw exception;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.ONE_TO_ONE;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{Byte.MAX_VALUE};
  }
}
