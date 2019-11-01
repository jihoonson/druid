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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 *
 */
public class DataSchema
{
  private static final Logger log = new Logger(DataSchema.class);
  private static final Pattern INVALIDCHARS = Pattern.compile("(?s).*[^\\S ].*");
  private final String dataSource;
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;
  private final TransformSpec transformSpec;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("transformSpec") TransformSpec transformSpec,
      @Deprecated @JsonProperty("parser") @Nullable Map<String, Object> parser,
      @JacksonInject ObjectMapper objectMapper
  )
  {
  }

  private DataSchema(
      String dataSource,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec,
      TransformSpec transformSpec
  )
  {
    this.timestampSpec = Preconditions.checkNotNull(timestampSpec, "timestampSpec");
    this.dimensionsSpec = computeDimensionsSpec(timestampSpec, Preconditions.checkNotNull(dimensionsSpec), aggregators);
    this.transformSpec = transformSpec == null ? TransformSpec.NONE : transformSpec;

    validateDatasourceName(dataSource);
    this.dataSource = dataSource;

    if (granularitySpec == null) {
      log.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
      this.granularitySpec = new UniformGranularitySpec(null, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }

    if (aggregators != null && aggregators.length != 0) {
      // validate for no duplication
      Set<String> names = new HashSet<>();
      for (AggregatorFactory factory : aggregators) {
        if (!names.add(factory.getName())) {
          throw new IAE("duplicate aggregators found with name [%s].", factory.getName());
        }
      }
    } else if (this.granularitySpec.isRollup()) {
      log.warn("No metricsSpec has been specified. Are you sure this is what you want?");
    }

    this.aggregators = aggregators == null ? new AggregatorFactory[]{} : aggregators;
  }

  private static void validateDatasourceName(String dataSource)
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(dataSource),
        "dataSource cannot be null or empty. Please provide a dataSource."
    );
    Matcher m = INVALIDCHARS.matcher(dataSource);
    Preconditions.checkArgument(
        !m.matches(),
        "dataSource cannot contain whitespace character except space."
    );
    Preconditions.checkArgument(!dataSource.contains("/"), "dataSource cannot contain the '/' character.");
  }

  private static DimensionsSpec computeDimensionsSpec(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggregators
  )
  {
    final Set<String> dimensionExclusions = new HashSet<>();

    final String timestampColumn = timestampSpec.getTimestampColumn();
    if (!(dimensionsSpec.hasCustomDimensions() && dimensionsSpec.getDimensionNames().contains(timestampColumn))) {
      dimensionExclusions.add(timestampColumn);
    }

    for (AggregatorFactory aggregator : aggregators) {
      dimensionExclusions.addAll(aggregator.requiredFields());
      dimensionExclusions.add(aggregator.getName());
    }

    final Set<String> metSet = Arrays.stream(aggregators).map(AggregatorFactory::getName).collect(Collectors.toSet());
    final Set<String> dimSet = new HashSet<>(dimensionsSpec.getDimensionNames());
    final Set<String> overlap = Sets.intersection(metSet, dimSet);
    if (!overlap.isEmpty()) {
      throw new IAE(
          "Cannot have overlapping dimensions and metrics of the same name. Please change the name of the metric. Overlap: %s",
          overlap
      );
    }

    return dimensionsSpec.withDimensionExclusions(Sets.difference(dimensionExclusions, dimSet));
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
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

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    return new DataSchema(dataSource, timestampSpec, dimensionsSpec, aggregators, granularitySpec, transformSpec);
  }

  public DataSchema withTransformSpec(TransformSpec transformSpec)
  {
    return new DataSchema(dataSource, timestampSpec, dimensionsSpec, aggregators, granularitySpec, transformSpec);
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", timestampSpec=" + timestampSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", granularitySpec=" + granularitySpec +
           ", transformSpec=" + transformSpec +
           '}';
  }
}
