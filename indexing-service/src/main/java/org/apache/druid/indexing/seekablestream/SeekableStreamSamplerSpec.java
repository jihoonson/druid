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

package org.apache.druid.indexing.seekablestream;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse;
import org.apache.druid.indexing.overlord.sampler.SamplerSpec;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;

public abstract class SeekableStreamSamplerSpec<PartitionIdType, SequenceOffsetType> implements SamplerSpec
{
  @Nullable
  private final DataSchema dataSchema;
  private final InputSourceSampler inputSourceSampler;

  protected final SeekableStreamSupervisorIOConfig ioConfig;
  @Nullable
  protected final SeekableStreamSupervisorTuningConfig tuningConfig;
  protected final SamplerConfig samplerConfig;

  public SeekableStreamSamplerSpec(
      final SeekableStreamSupervisorSpec ingestionSpec,
      @Nullable final SamplerConfig samplerConfig,
      final InputSourceSampler inputSourceSampler
  )
  {
    this.dataSchema = Preconditions.checkNotNull(ingestionSpec, "[spec] is required").getDataSchema();
    this.ioConfig = Preconditions.checkNotNull(ingestionSpec.getIoConfig(), "[spec.ioConfig] is required");
    this.tuningConfig = ingestionSpec.getTuningConfig();
    this.samplerConfig = samplerConfig == null ? SamplerConfig.empty() : samplerConfig;
    this.inputSourceSampler = inputSourceSampler;
  }

  @Override
  public SamplerResponse sample()
  {
    final InputSource inputSource = new RecordSupplierInputSource<>(
        ioConfig.getStream(),
        createRecordSupplier(),
        ioConfig.isUseEarliestSequenceNumber()
    );
    final ParseSpec parseSpec;
    if (dataSchema != null) {
      parseSpec = dataSchema.getParser() == null ? null : dataSchema.getParser().getParseSpec();
    } else {
      parseSpec = null;
    }
    final InputFormat inputFormat = ioConfig.getInputFormat(parseSpec);
    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }

  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType> createRecordSupplier();
}
