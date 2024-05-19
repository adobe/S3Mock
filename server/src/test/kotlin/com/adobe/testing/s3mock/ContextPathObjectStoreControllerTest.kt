/*
 *  Copyright 2017-2024 Adobe.
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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.Buckets
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.MultipartStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import java.nio.file.Paths
import java.time.Instant

@MockBean(classes = [KmsKeyStore::class, ObjectService::class, MultipartService::class, MultipartStore::class])
@SpringBootTest(
  properties = ["com.adobe.testing.s3mock.contextPath=s3-mock"],
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
internal class ContextPathObjectStoreControllerTest : BaseControllerTest() {
  @MockBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  @Throws(Exception::class)
  fun testListBuckets_Ok() {
    val bucketList = listOf(
      TEST_BUCKET,
      Bucket(Paths.get("/tmp/foo/2"), "testBucket1", Instant.now().toString())
    )
    val expected = ListAllMyBucketsResult(TEST_OWNER, Buckets(bucketList))
    whenever(bucketService.listBuckets()).thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/s3-mock/",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  companion object {
    private val TEST_OWNER = Owner("123", "s3-mock-file-store")

    private const val TEST_BUCKET_NAME = "testBucket"
    private val TEST_BUCKET = Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString())
  }
}


