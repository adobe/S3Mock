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

package com.adobe.testing.s3mock.its

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

/**
 * Chunked encoding with signing is only active in AWS SDK v2 when endpoint is http
 */
internal class AwsChunkedEndcodingITV2 : S3TestBase() {

  private val s3ClientV2 = createS3ClientV2(serviceEndpointHttp)

  /**
   * Unfortunately the S3 API does not persist or return data that would let us verify if signed and chunked encoding
   * was actually used for the putObject request.
   * This was manually validated through the debugger.
   */
  @Test
  @S3VerifiedFailure(
    year = 2023,
    reason = "Only works with http endpoints"
  )
  fun `put object with checksum returns correct checksum, get object returns checksum`(testInfo: TestInfo) {
    val bucket = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), Algorithm.SHA256)

    val putObjectResponse = s3ClientV2.putObject(
      {
        it.bucket(bucket)
        it.key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA256)
      },
      RequestBody.fromFile(uploadFile)
    )

    putObjectResponse.checksumSHA256().also {
      assertThat(it).isNotBlank
      assertThat(it).isEqualTo(expectedChecksum)
    }

    s3ClientV2.getObject {
      it.bucket(bucket)
      it.key(UPLOAD_FILE_NAME)
    }.also { getObjectResponse ->
      assertThat(getObjectResponse.response().eTag()).isEqualTo(expectedEtag)
      assertThat(getObjectResponse.response().contentLength()).isEqualTo(uploadFile.length())

      getObjectResponse.response().checksumSHA256().also {
        assertThat(it).isNotBlank
        assertThat(it).isEqualTo(expectedChecksum)
      }
    }
  }

  /**
   * Unfortunately the S3 API does not persist or return data that would let us verify if signed and chunked encoding
   * was actually used for the putObject request.
   * This was manually validated through the debugger.
   */
  @Test
  @S3VerifiedFailure(
    year = 2023,
    reason = "Only works with http endpoints"
  )
  fun `put object creates correct etag, get object returns etag`(testInfo: TestInfo) {
    val bucket = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    s3ClientV2.putObject(
      {
        it.bucket(bucket)
        it.key(UPLOAD_FILE_NAME)
      },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.getObject {
      it.bucket(bucket)
      it.key(UPLOAD_FILE_NAME)
    }.also {
      assertThat(it.response().eTag()).isEqualTo(expectedEtag)
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }
}
