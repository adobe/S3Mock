/*
 *  Copyright 2017-2018 Adobe.
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

package com.adobe.testing.s3mock.junit5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Tests and demonstrates the usage of the {@link S3MockExtension}.
 */
@ExtendWith(S3MockExtension.class)
public class S3MockExtensionDeclarativeTest {

  private static final String BUCKET_NAME = "mydemotestbucket";
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   * @param s3Client Client injected by the test framework
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadObject(final AmazonS3 s3Client) throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    final InputStream uploadFileIS = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIS);
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    uploadFileIS.close();
    s3Object.close();

    assertEquals(uploadHash, downloadedHash, "Up- and downloaded Files should have equal Hashes");
  }

  @Nested
  class nestedTest {

    @Test
    void nestedTestShouldNotStartSecondInstanceOfMock(AmazonS3 s3Client){
      // Do things with the bucket...
      assertNotNull(s3Client);
    }

  }
}
