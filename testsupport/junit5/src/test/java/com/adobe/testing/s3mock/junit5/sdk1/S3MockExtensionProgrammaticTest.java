/*
 *  Copyright 2017-2022 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adobe.testing.s3mock.junit5.sdk1;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests and demonstrates the usage of the {@link S3MockExtension} for the SDK v1.
 */
class S3MockExtensionProgrammaticTest {

  @RegisterExtension
  static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
      .withSecureConnection(false).build();

  private static final String BUCKET_NAME = "mydemotestbucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  private final AmazonS3 s3Client = S3_MOCK.createS3Client();

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndDownloadObject() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    final InputStream uploadFileIs = Files.newInputStream(uploadFile.toPath());
    final String uploadDigest = DigestUtil.getHexDigest(uploadFileIs);
    final String downloadedDigest = DigestUtil.getHexDigest(s3Object.getObjectContent());
    uploadFileIs.close();
    s3Object.close();

    assertThat(uploadDigest).isEqualTo(downloadedDigest).as(
        "Up- and downloaded Files should have equal digests");
  }
}
