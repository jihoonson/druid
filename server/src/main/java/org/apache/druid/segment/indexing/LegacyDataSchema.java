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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

public class LegacyDataSchema
{
  private final String dataSource;
  private final Map<String, Object> parser;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;
  private final TransformSpec transformSpec;
  private final ObjectMapper objectMapper;

  @JsonCreator
  public LegacyDataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") @Nullable GranularitySpec granularitySpec,
      @JsonProperty("transformSpec") @Nullable TransformSpec transformSpec,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    this.dataSource = dataSource;
    this.parser = parser;
    this.aggregators = aggregators;
    this.granularitySpec = granularitySpec;
    this.transformSpec = transformSpec == null ? TransformSpec.NONE : transformSpec;
    this.objectMapper = objectMapper;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("parser")
  public Map<String, Object> getParserMap()
  {
    return parser;
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  public DataSchema toDataSchema()
  {
    final InputRowParser inputRowParser = objectMapper.convertValue(this.parser, InputRowParser.class);
    return new DataSchema(
        dataSource,
        inputRowParser.getParseSpec().getTimestampSpec(),
        inputRowParser.getParseSpec().getDimensionsSpec(),
        aggregators,
        granularitySpec,
        transformSpec
    );
  }

  @Override
  public String toString()
  {
    return "LegacyDataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", parser=" + parser +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", granularitySpec=" + granularitySpec +
           ", transformSpec=" + transformSpec +
           '}';
  }
}
