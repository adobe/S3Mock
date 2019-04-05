/*
 *  Copyright 2017-2019 Adobe.
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

package com.adobe.testing.s3mock.junit5.sdk2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.adobe.testing.s3mock.util.HashUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Tests and demonstrates the usage of the {@link S3MockExtension}
 * for the SDK v2.
 * */
@ExtendWith(S3MockExtension.class)
class S3MockExtensionDeclarativeTest {

  private static final String BUCKET_NAME = "mydemotestbucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @param s3Client Client injected by the test framework
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndDownloadObject(final S3Client s3Client) throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
    s3Client.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(uploadFile.getName()).build(),
        RequestBody.fromFile(uploadFile));

    final ResponseInputStream<GetObjectResponse> response =
        s3Client.getObject(
            GetObjectRequest.builder().bucket(BUCKET_NAME).key(uploadFile.getName()).build());

    final InputStream uploadFileIs = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIs);
    final String downloadedHash = HashUtil.getDigest(response);
    uploadFileIs.close();
    response.close();

    assertEquals(uploadHash, downloadedHash, "Up- and downloaded Files should have equal Hashes");
  }

  @Nested
  class NestedTest {

    @Test
    void nestedTestShouldNotStartSecondInstanceOfMock(final S3Client s3Client) {
      assertNotNull(s3Client);
    }
  }
}
