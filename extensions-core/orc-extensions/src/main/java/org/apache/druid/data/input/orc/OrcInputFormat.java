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

package org.apache.druid.data.input.orc;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.ObjectReader;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;

public class OrcInputFormat extends NestedInputFormat
{
  private final Configuration conf;

  @JsonCreator
  public OrcInputFormat(
      @JsonProperty("flattenSpec") JSONPathSpec flattenSpec,
      @JacksonInject Configuration conf // TODO:
  )
  {
    super(flattenSpec);
    this.conf = conf;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public ObjectReader createReader(InputRowSchema inputRowSchema)
  {
    return new OrcReader(conf, inputRowSchema, getFlattenSpec(), true);
  }
}
