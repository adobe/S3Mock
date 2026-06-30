/*
 *  Copyright 2017-2026 Adobe.
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
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CorsIT : S3TestBase() {
  private val httpClient: HttpClient = createHttpClient()

  @Test
  @S3VerifiedFailure(
    year = 2024,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testPutObject_cors(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val optionsRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$bucketName/testObjectName"))
        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
        .header("Origin", "http://localhost/")
        .build()
    val optionsResponse = httpClient.send(optionsRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(optionsResponse.headers().firstValue("Allow").orElse(null)).contains("PUT")

    val byteArray = UUID.randomUUID().toString().toByteArray()
    val expectedEtag = "\"${DigestUtil.hexDigest(byteArray.inputStream())}\""
    val putObject =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$bucketName/testObjectName"))
        .PUT(HttpRequest.BodyPublishers.ofByteArray(byteArray))
        .header("Origin", "http://localhost/")
        .build()

    val putResponse = httpClient.send(putObject, HttpResponse.BodyHandlers.discarding())
    assertThat(putResponse.statusCode()).isEqualTo(200)
    assertThat(putResponse.headers().firstValue("ETag").orElse(null)).isEqualTo(expectedEtag)
    assertThat(putResponse.headers().firstValue("Access-Control-Allow-Origin").orElse(null)).isEqualTo("*")
    assertThat(putResponse.headers().firstValue("Access-Control-Expose-Headers").orElse(null)).isEqualTo("*")
  }

  @Test
  @S3VerifiedFailure(
    year = 2024,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testGetBucket_cors(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val httpOptions =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket"))
        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
        .header("Origin", "http://someurl.com")
        .header("Access-Control-Request-Method", "GET")
        .header("Access-Control-Request-Headers", "Content-Type, x-requested-with")
        .build()

    val response = httpClient.send(httpOptions, HttpResponse.BodyHandlers.discarding())
    assertThat(response.headers().firstValue("Access-Control-Allow-Origin").orElse(null))
      .isEqualTo("http://someurl.com")
    assertThat(response.headers().firstValue("Access-Control-Allow-Methods").orElse(null)).isEqualTo("GET")
    assertThat(response.headers().firstValue("Access-Control-Allow-Headers").orElse(null))
      .isEqualTo("Content-Type, x-requested-with")
    assertThat(response.headers().firstValue("Access-Control-Allow-Credentials").orElse(null)).isEqualTo("true")
    assertThat(response.headers().firstValue("Allow").orElse(null)).contains("GET")
  }
}
