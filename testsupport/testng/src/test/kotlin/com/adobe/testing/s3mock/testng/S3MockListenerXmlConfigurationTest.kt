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
import java.io.File
import java.nio.file.Files

@Test
class S3MockListenerXmlConfigurationTest {
  private val s3Client = S3Mock.getInstance().createS3ClientV2()

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  @Throws(Exception::class)
  fun shouldUploadAndDownloadObject() {
    val uploadFile = File(UPLOAD_FILE_NAME)

    s3Client.createBucket {
      it.bucket(BUCKET_NAME)
    }
    s3Client.putObject(
      {
        it.bucket(BUCKET_NAME)
        it.key(uploadFile.getName())
      },
      RequestBody.fromFile(uploadFile)
    )

    s3Client.getObject {
      it.bucket(BUCKET_NAME)
      it.key(uploadFile.getName())
    }.use { response ->
      val uploadDigest = Files.newInputStream(uploadFile.toPath()).use {
        DigestUtil.hexDigest(it)
      }
      val downloadedDigest = DigestUtil.hexDigest(response)
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  companion object {
    private const val BUCKET_NAME = "my-demo-test-bucket"
    private const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"
  }
}
