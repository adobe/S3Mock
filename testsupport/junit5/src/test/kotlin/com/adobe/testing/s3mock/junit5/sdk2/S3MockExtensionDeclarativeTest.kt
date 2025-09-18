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
package com.adobe.testing.s3mock.junit5.sdk2

import com.adobe.testing.s3mock.junit5.S3MockExtension
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import java.io.File

/**
 * Tests and demonstrates the usage of the [S3MockExtension]
 * for the SDK v2.
 */
@ExtendWith(S3MockExtension::class)
internal class S3MockExtensionDeclarativeTest {
  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @param s3Client Client injected by the test framework
   * @throws Exception if FileStreams cannot be read
   */
  @Test
  @Throws(Exception::class)
  fun shouldUploadAndDownloadObject(s3Client: S3Client) {
    val bucketName = BUCKET_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)

    s3Client.createBucket {
      it.bucket(bucketName)
    }
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(uploadFile.getName())
      },
      RequestBody.fromFile(uploadFile)
    )

    s3Client.getObject {
      it.bucket(bucketName)
      it.key(uploadFile.getName())
    }.use { response ->
      val uploadDigest = uploadFile.inputStream().use {
        DigestUtil.hexDigest(it)
      }
      val downloadedDigest = DigestUtil.hexDigest(response)
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  @Nested
  internal inner class NestedTest {
    @Test
    fun nestedTestShouldNotStartSecondInstanceOfMock(s3Client: S3Client?) {
      assertThat(s3Client).isNotNull()
    }
  }

  companion object {
    private const val BUCKET_NAME = "s3-mock-extension-declarative-test"
    private const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"
  }
}
