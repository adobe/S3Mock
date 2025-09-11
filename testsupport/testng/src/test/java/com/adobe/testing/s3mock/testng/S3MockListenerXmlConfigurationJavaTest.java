/*
 *  Copyright 2017-2025 Adobe.
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

package com.adobe.testing.s3mock.testng;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.File;
import java.nio.file.Files;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Test
public class S3MockListenerXmlConfigurationJavaTest {

  private static final String BUCKET_NAME = "my-demo-test-bucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  private final S3Client s3Client = S3Mock.getInstance().createS3ClientV2();

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadObject() throws Exception {
    var uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
    s3Client.putObject(
        PutObjectRequest
            .builder()
            .bucket(BUCKET_NAME)
            .key(uploadFile.getName())
            .build(),
        RequestBody.fromFile(uploadFile)
    );

    var s3Object = s3Client.getObject(
        GetObjectRequest
            .builder()
            .bucket(BUCKET_NAME)
            .key(uploadFile.getName())
            .build()
    );

    var uploadFileIs = Files.newInputStream(uploadFile.toPath());
    var uploadDigest = DigestUtil.hexDigest(uploadFileIs);
    var downloadedDigest = DigestUtil.hexDigest(s3Object);
    uploadFileIs.close();
    s3Object.close();

    assertThat(uploadDigest)
        .as("Up- and downloaded Files should have equal digests")
        .isEqualTo(downloadedDigest);
  }
}
