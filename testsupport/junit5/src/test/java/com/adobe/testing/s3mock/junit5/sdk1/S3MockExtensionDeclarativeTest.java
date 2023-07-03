/*
 *  Copyright 2017-2023 Adobe.
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
import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests and demonstrates the usage of the {@link S3MockExtension} for the SDK v1.
 */
@ExtendWith(S3MockExtension.class)
class S3MockExtensionDeclarativeTest {

  private static final String BUCKET_NAME = "mydemotestbucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @param s3Client Client injected by the test framework
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndDownloadObject(final AmazonS3 s3Client) throws Exception {
    var uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    var s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    var uploadFileIs = Files.newInputStream(uploadFile.toPath());
    var uploadDigest = DigestUtil.hexDigest(uploadFileIs);
    var downloadedDigest = DigestUtil.hexDigest(s3Object.getObjectContent());
    uploadFileIs.close();
    s3Object.close();

    assertThat(uploadDigest).as("Up- and downloaded Files should have equal digests")
        .isEqualTo(downloadedDigest);
  }

  @Nested
  class NestedTest {

    @Test
    void nestedTestShouldNotStartSecondInstanceOfMock(final AmazonS3 s3Client) {
      assertThat(s3Client).isNotNull();
    }
  }
}
