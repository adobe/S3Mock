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
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpOptions
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.util.UUID

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CorsIT : S3TestBase() {
  private val httpClient: CloseableHttpClient = createHttpClient()

  @Test
  @S3VerifiedFailure(
    year = 2024,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testPutObject_cors(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val optionsRequest =
      HttpOptions("$serviceEndpoint/$bucketName/testObjectName").apply {
        addHeader("Origin", "http://localhost/")
      }
    httpClient.execute(optionsRequest).also {
      assertThat(it.getFirstHeader("Allow").value).contains("PUT")
    }

    val byteArray = UUID.randomUUID().toString().toByteArray()
    val expectedEtag = "\"${DigestUtil.hexDigest(byteArray)}\""
    val putObject =
      HttpPut("$serviceEndpoint/$bucketName/testObjectName").apply {
        entity = ByteArrayEntity(byteArray)
        addHeader("Origin", "http://localhost/")
      }

    httpClient.execute(putObject).use {
      assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      assertThat(it.getFirstHeader("ETag").value).isEqualTo(expectedEtag)
      assertThat(it.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("*")
      assertThat(it.getFirstHeader("Access-Control-Expose-Headers").value).isEqualTo("*")
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2024,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testGetBucket_cors(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val httpOptions =
      HttpOptions("$serviceEndpoint/$targetBucket").apply {
        addHeader("Origin", "http://someurl.com")
        addHeader("Access-Control-Request-Method", "GET")
        addHeader("Access-Control-Request-Headers", "Content-Type, x-requested-with")
      }

    httpClient.execute(httpOptions).use {
      assertThat(it.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("http://someurl.com")
      assertThat(it.getFirstHeader("Access-Control-Allow-Methods").value).isEqualTo("GET")
      assertThat(it.getFirstHeader("Access-Control-Allow-Headers").value)
        .isEqualTo("Content-Type, x-requested-with")
      assertThat(it.getFirstHeader("Access-Control-Allow-Credentials").value).isEqualTo("true")
      assertThat(it.getFirstHeader("Allow").value).contains("GET")
    }
  }
}
