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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputEntitySampler;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusJson;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class OrcReader implements InputEntityReader, InputEntitySampler
{
  private final Configuration conf;
  private final InputRowSchema inputRowSchema;
  private final ObjectFlattener<OrcStruct> orcStructFlattener;
  private final byte[] buffer = new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE];

  OrcReader(Configuration conf, InputRowSchema inputRowSchema, JSONPathSpec flattenSpec)
  {
    this.conf = conf;
    this.inputRowSchema = inputRowSchema;
    this.orcStructFlattener = ObjectFlatteners.create(flattenSpec, new OrcStructFlattenerMaker(false));
  }

  private CloseableIterator<OrcStruct> createOrcStructIterator(InputEntity<?> source, File temporaryDirectory)
      throws IOException
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
    return new CloseableIterator<OrcStruct>()
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
      public OrcStruct next()
      {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        hasNext = null;
        return value;
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRow> read(InputEntity source, File temporaryDirectory) throws IOException
  {
    return createOrcStructIterator(source, temporaryDirectory).map(orcStruct -> MapInputRowParser.parse(
        inputRowSchema.getTimestampSpec(),
        inputRowSchema.getDimensionsSpec(),
        orcStructFlattener.flatten(orcStruct)
    ));
  }

  @Override
  public CloseableIterator<InputRowListPlusJson> sample(InputEntity<?> source, File temporaryDirectory)
      throws IOException
  {
    return createOrcStructIterator(source, temporaryDirectory).map(orcStruct -> {
      String json;
      try {
        json = SAMPLER_JSON_WRITER.writeValueAsString(orcStruct2Map(orcStruct));
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      try {
        final InputRow inputRow = MapInputRowParser.parse(
            inputRowSchema.getTimestampSpec(),
            inputRowSchema.getDimensionsSpec(),
            orcStructFlattener.flatten(orcStruct)
        );
        return InputRowListPlusJson.ofJson(inputRow, json);
      }
      catch (ParseException e) {
        return InputRowListPlusJson.of(json, e);
      }
    });
  }

  private Map<String, Object> orcStruct2Map(OrcStruct orcStruct)
  {
    final OrcStructConverter converter = new OrcStructConverter(false);
    final Map<String, Object> converted = new HashMap<>();
    for (String fieldName : orcStruct.getSchema().getFieldNames()) {
      converted.put(fieldName, converter.convertRootField(orcStruct, fieldName));
    }
    return converted;
  }
}
