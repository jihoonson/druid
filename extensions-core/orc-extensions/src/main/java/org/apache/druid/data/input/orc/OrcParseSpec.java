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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputFormat;
import org.apache.druid.data.input.impl.NestedDataParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;

public class OrcParseSpec extends NestedDataParseSpec<JSONPathSpec>
{
  private final Configuration conf;

  @JsonCreator
  public OrcParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("flattenSpec") JSONPathSpec flattenSpec,
      @JacksonInject Configuration conf
  )
  {
    super(
        timestampSpec,
        dimensionsSpec != null ? dimensionsSpec : DimensionsSpec.EMPTY,
        flattenSpec != null ? flattenSpec : JSONPathSpec.DEFAULT
    );
    this.conf = conf;
  }

  @Override
  public InputFormat toInputFormat()
  {
    return new OrcInputFormat(getFlattenSpec(), conf);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new OrcParseSpec(spec, getDimensionsSpec(), getFlattenSpec(), conf);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new OrcParseSpec(getTimestampSpec(), spec, getFlattenSpec(), conf);
  }

  @Override
  public String toString()
  {
    return "OrcParseSpec{" +
           "timestampSpec=" + getTimestampSpec() +
           ", dimensionsSpec=" + getDimensionsSpec() +
           ", flattenSpec=" + getFlattenSpec() +
           "}";
  }
}
