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
package com.adobe.testing.s3mock.testng

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test
import software.amazon.awssdk.core.sync.RequestBody
import java.nio.file.Files
import java.nio.file.Paths

@Test
class S3MockListenerXmlConfigurationTest {
  private val s3Client = S3Mock.getInstance().createS3ClientV2()

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   */
  @Test
  fun shouldUploadAndDownloadObject() {
    val uploadPath = Paths.get(UPLOAD_FILE_NAME)
    val key = uploadPath.fileName.toString()

    s3Client.createBucket { it.bucket(BUCKET_NAME) }

    s3Client.putObject(
      { it.bucket(BUCKET_NAME).key(key) },
      RequestBody.fromFile(uploadPath)
    )

    s3Client.getObject { it.bucket(BUCKET_NAME).key(key) }.use { response ->
      val uploadDigest = Files.newInputStream(uploadPath).use(DigestUtil::hexDigest)
      val downloadedDigest = DigestUtil.hexDigest(response)
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  companion object {
    private const val BUCKET_NAME = "s3-mock-listener-xml-configuration-test"
    private const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"
  }
}
