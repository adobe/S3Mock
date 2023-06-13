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

package com.adobe.testing.s3mock.junit4;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.util.DigestUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;
import java.nio.file.Files;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests and demonstrates the usage of the {@link S3MockRule}.
 */
public class S3MockRuleTest {

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private static final String BUCKET_NAME = "my-demo-test-bucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  private final AmazonS3 s3Client = S3_MOCK_RULE.createS3Client();

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadObject() throws Exception {
    var uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    var s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    var uploadFileIs = Files.newInputStream(uploadFile.toPath());
    var uploadHash = DigestUtil.hexDigest(uploadFileIs);
    var downloadedHash = DigestUtil.hexDigest(s3Object.getObjectContent());
    uploadFileIs.close();
    s3Object.close();

    assertThat(uploadHash).as("Up- and downloaded Files should have equal Hashes")
        .isEqualTo(downloadedHash);
  }
}
