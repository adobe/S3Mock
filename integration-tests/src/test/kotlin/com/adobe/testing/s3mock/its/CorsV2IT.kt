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

package com.adobe.testing.s3mock.its

import com.adobe.testing.s3mock.util.DigestUtil
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpOptions
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException
import java.util.UUID


/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CorsV2IT : S3TestBase() {
  private lateinit var httpClient: CloseableHttpClient

  @BeforeEach
  fun setupHttpClient() {
    httpClient = HttpClients.createDefault()
  }

  @AfterEach
  @Throws(IOException::class)
  fun shutdownHttpClient() {
    httpClient.close()
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_cors(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val httpclient = HttpClientBuilder.create().build()
    val optionsRequest = HttpOptions("/${bucketName}/testObjectName")
    optionsRequest.addHeader("Origin", "http://localhost/")
    val optionsResponse: HttpResponse = httpclient.execute(HttpHost(
      host, httpPort
    ), optionsRequest)
    assertThat(optionsResponse.getFirstHeader("Allow").value).contains("PUT")

    val putObject = HttpPut("/$bucketName/testObjectName")
    val byteArray = UUID.randomUUID().toString().toByteArray()
    val expectedEtag = "\"${DigestUtil.hexDigest(byteArray)}\""
    putObject.entity = ByteArrayEntity(byteArray)
    putObject.addHeader("Origin", "http://localhost/")
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    assertThat(putObjectResponse.getFirstHeader("ETag").value).isEqualTo(expectedEtag)
    assertThat(putObjectResponse.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("*")
    assertThat(putObjectResponse.getFirstHeader("Access-Control-Expose-Headers").value).isEqualTo("*")
  }

  @Test
  fun testGetBucket_cors(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val httpOptions = HttpOptions("/$targetBucket")
    httpOptions.addHeader(BasicHeader("Origin", "http://someurl.com"))
    httpOptions.addHeader(BasicHeader("Access-Control-Request-Method", "GET"))
    httpOptions.addHeader(BasicHeader("Access-Control-Request-Headers", "Content-Type, x-requested-with"))
    val response: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), httpOptions
    )
    assertThat(response.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("http://someurl.com")
    assertThat(response.getFirstHeader("Access-Control-Allow-Methods").value).isEqualTo("GET")
    assertThat(response.getFirstHeader("Access-Control-Allow-Headers").value)
      .isEqualTo("Content-Type, x-requested-with")
    assertThat(response.getFirstHeader("Access-Control-Allow-Credentials").value).isEqualTo("true")
    assertThat(response.getFirstHeader("Allow").value).contains("GET")
  }
}
