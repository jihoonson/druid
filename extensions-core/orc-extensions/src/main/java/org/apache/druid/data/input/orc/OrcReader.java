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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.data.input.SplitSource.CleanableFile;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class OrcReader implements SplitReader
{
  private final Configuration conf;
  private final TimestampSpec timestampSpec;
  private final List<String> dimensions;
  private final Set<String> dimensionExclusions;
  private final ObjectFlattener<OrcStruct> orcStructFlattener;
  private final byte[] buffer = new byte[SplitSource.DEFAULT_FETCH_BUFFER_SIZE];

  OrcReader(
      Configuration conf,
      TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      JSONPathSpec flattenSpec
  )
  {
    this.conf = conf;
    this.timestampSpec = timestampSpec;
    this.dimensions = dimensionsSpec != null ? dimensionsSpec.getDimensionNames() : Collections.emptyList();
    this.dimensionExclusions = dimensionsSpec != null
                               ? dimensionsSpec.getDimensionExclusions()
                               : Collections.emptySet();
    this.orcStructFlattener = ObjectFlatteners.create(flattenSpec, new OrcStructFlattenerMaker(false));
  }

  @Override
  public CloseableIterator<InputRow> read(SplitSource source, File temporaryDirectory) throws IOException
  {
    Closer closer = Closer.create();
    final CleanableFile file = closer.register(source.fetch(temporaryDirectory, buffer));
    final Path path = new Path(file.file().toURI());

    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    final Reader reader;
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      reader = closer.register(OrcFile.createReader(path, OrcFile.readerOptions(conf)));
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
    // TODO: build schema from flattenSpec
    //       final RecordReader recordReader = reader.rows(reader.options().schema());
    final TypeDescription schema = reader.getSchema();
    final RecordReader batchReader = reader.rows(reader.options());
    final OrcMapredRecordReader<OrcStruct> recordReader = new OrcMapredRecordReader<>(batchReader, schema);
    closer.register(recordReader::close);

    return new CloseableIterator<InputRow>()
    {
      final NullWritable key = recordReader.createKey();
      final OrcStruct value = recordReader.createValue();
      Boolean hasNext = null;

      @Override
      public boolean hasNext()
      {
        if (hasNext == null) {
          try {
            hasNext = recordReader.next(key, value);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return hasNext;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        hasNext = null;
        return MapInputRowParser.parse(
            timestampSpec,
            dimensions,
            dimensionExclusions,
            orcStructFlattener.flatten(value)
        );
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowPlusRaw> sample(SplitSource source, File temporaryDirectory)
  {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
