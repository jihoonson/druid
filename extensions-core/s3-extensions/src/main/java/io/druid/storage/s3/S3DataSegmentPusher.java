/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class S3DataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3DataSegmentPusher.class);

  private final AmazonS3Client s3Client;
  private final TransferManager transferManager;
  private final S3DataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3DataSegmentPusher(
      AmazonS3Client s3Client,
      S3DataSegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.s3Client = s3Client;
    // TODO: config for thread pool size
    this.transferManager = new TransferManager(s3Client);
    this.config = config;
    this.jsonMapper = jsonMapper;

    log.info("Configured S3 as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    if (config.isUseS3aSchema()) {
      return StringUtils.format("s3a://%s/%s", config.getBucket(), config.getBaseKey());
    }
    return StringUtils.format("s3n://%s/%s", config.getBucket(), config.getBaseKey());
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return ImmutableList.of("druid.s3");
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    final String s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), getStorageDir(inSegment));

    log.info("Copying segment[%s] to S3 at location[%s]", inSegment.getIdentifier(), s3Path);

    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

    try {
      return S3Utils.retryS3Operation(
          new Callable<DataSegment>()
          {
            @Override
            public DataSegment call() throws Exception
            {
              final String outputBucket = config.getBucket();
              final String s3DescriptorPath = S3Utils.descriptorPathForSegmentPath(s3Path);
              final DataSegment outSegment = inSegment
                  .withSize(indexSize)
                  .withLoadSpec(makeLoadSpec(outputBucket, s3Path))
                  .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

              // prepare uploads
              final List<ToUpload> toUploads = new ArrayList<>(2);
              toUploads.add(new ToUpload(() -> zipOutFile, outputBucket, s3Path));
              toUploads.add(
                  new ToUpload(
                      () -> {
                        try {
                          File descriptorFile = File.createTempFile("druid", "descriptor.json");
                          // Avoid using Guava in DataSegmentPushers because they might be used with very diverse Guava
                          // versions in runtime, and because Guava deletes methods over time, that causes
                          // incompatibilities.
                          Files.write(descriptorFile.toPath(), jsonMapper.writeValueAsBytes(outSegment));
                          return descriptorFile;
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      },
                      outputBucket,
                      s3DescriptorPath
                  )
              );

              // upload files
              final List<Uploading> uploadings = toUploads
                  .stream()
                  .map(
                      toUpload -> {
                        final File file = toUpload.getFile();
                        final PutObjectRequest request = new PutObjectRequest(
                            toUpload.bucket,
                            toUpload.key,
                            file
                        );

                        if (!config.getDisableAcl()) {
                          request.setAccessControlList(
                              S3Utils.grantFullControlToBucketOwver(s3Client, toUpload.bucket)
                          );
                        }

                        log.info(
                            "Schedule to push [%s] to bucket[%s] and key[%s].",
                            file.getName(),
                            toUpload.bucket,
                            toUpload.key
                        );
                        return new Uploading(toUpload, transferManager.upload(request));
                      }
                  )
                  .collect(Collectors.toList());

              for (Uploading uploading : uploadings) {
                log.info(
                    "Wait for [%s] to be pushed to bucket[%s] and key[%s]",
                    uploading.toUpload.getFile().getName(),
                    uploading.toUpload.bucket,
                    uploading.toUpload.key
                );
                uploading.waitForCompletion();
                log.info(
                    "[%s] is pushed to bucket[%s] and key[%s]",
                    uploading.toUpload.getFile().getName(),
                    uploading.toUpload.bucket,
                    uploading.toUpload.key
                );
              }

              // clean up
              uploadings.forEach(Uploading::close);

              return outSegment;
            }
          }
      );
    }
    catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    // remove the leading "/"
    return makeLoadSpec(finalIndexZipFilePath.getHost(), finalIndexZipFilePath.getPath().substring(1));
  }

  /**
   * Any change in loadSpec need to be reflected {@link io.druid.indexer.JobHelper#getURIFromSegment()}
   *
   */
  @SuppressWarnings("JavadocReference")
  private Map<String, Object> makeLoadSpec(String bucket, String key)
  {
    return ImmutableMap.<String, Object>of(
        "type",
        "s3_zip",
        "bucket",
        bucket,
        "key",
        key,
        "S3Schema",
        config.isUseS3aSchema() ? "s3a" : "s3n"
    );
  }

  private static class ToUpload implements Closeable
  {
    private final Supplier<File> fileSupplier;
    private final String bucket;
    private final String key;

    private File file;

    ToUpload(Supplier<File> fileSupplier, String bucket, String key)
    {
      this.fileSupplier = fileSupplier;
      this.bucket = bucket;
      this.key = key;
    }

    File getFile()
    {
      if (file == null) {
        file = fileSupplier.get();
      }
      return file;
    }

    @Override
    public void close()
    {
      if (file != null) {
        file.delete();
      }
    }
  }

  private static class Uploading implements Closeable
  {
    private final ToUpload toUpload;
    private final Upload upload;

    Uploading(ToUpload toUpload, Upload upload)
    {
      this.toUpload = toUpload;
      this.upload = upload;
    }

    UploadResult waitForCompletion() throws InterruptedException
    {
      return upload.waitForUploadResult();
    }

    @Override
    public void close()
    {
      toUpload.close();
    }
  }
}
