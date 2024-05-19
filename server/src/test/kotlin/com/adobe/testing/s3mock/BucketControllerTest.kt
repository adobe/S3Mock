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
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.Buckets
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.DefaultRetention
import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.dto.LifecycleExpiration
import com.adobe.testing.s3mock.dto.LifecycleRule
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.dto.ListBucketResult
import com.adobe.testing.s3mock.dto.ListBucketResultV2
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.ObjectLockRule
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Transition
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpParameters
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.verify
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
import org.springframework.web.util.UriComponentsBuilder
import java.nio.file.Paths
import java.time.Instant

@MockBean(
  classes = [KmsKeyStore::class, ObjectService::class, MultipartService::class, ObjectController::class, MultipartController::class]
)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class BucketControllerTest : BaseControllerTest() {
  @MockBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  @Throws(Exception::class)
  fun testListBuckets_Ok() {
    val bucketList = listOf(
      TEST_BUCKET,
      Bucket(Paths.get("/tmp/foo/2"), "test-bucket1", Instant.now().toString())
    )
    val expected = ListAllMyBucketsResult(TEST_OWNER, Buckets(bucketList))
    whenever(bucketService.listBuckets()).thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  @Throws(Exception::class)
  fun testListBuckets_Empty() {
    val expected = ListAllMyBucketsResult(TEST_OWNER, Buckets(emptyList()))
    whenever(bucketService.listBuckets()).thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  fun testHeadBucket_Ok() {
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.HEAD,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  fun testHeadBucket_NotFound() {
    doThrow(S3Exception.NO_SUCH_BUCKET).whenever(bucketService)
      .verifyBucketExists(ArgumentMatchers.anyString())

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
  }

  @Test
  fun testCreateBucket_Ok() {
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.PUT,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  fun testCreateBucket_InternalServerError() {
    whenever(bucketService.createBucket(TEST_BUCKET_NAME, false))
      .thenThrow(IllegalStateException("THIS IS EXPECTED"))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.PUT,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
  }

  @Test
  @Throws(Exception::class)
  fun testDeleteBucket_NoContent() {
    givenBucket()
    whenever(bucketService.isBucketEmpty(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.DELETE,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
  }

  @Test
  @Throws(Exception::class)
  fun testDeleteBucket_NotFound() {
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService).verifyBucketIsEmpty(ArgumentMatchers.anyString())

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.DELETE,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  @Throws(Exception::class)
  fun testDeleteBucket_Conflict() {
    givenBucket()
    doThrow(S3Exception.BUCKET_NOT_EMPTY)
      .whenever(bucketService).verifyBucketIsEmpty(ArgumentMatchers.anyString())

    whenever(bucketService.getS3Objects(TEST_BUCKET_NAME, null))
      .thenReturn(
        listOf(
          S3Object(
            null, null, null, null, null, null, null
          )
        )
      )

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.DELETE,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.CONFLICT)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.BUCKET_NOT_EMPTY)))
  }

  @Test
  fun testDeleteBucket_InternalServerError() {
    givenBucket()

    doThrow(IllegalStateException("THIS IS EXPECTED"))
      .whenever(bucketService).verifyBucketIsEmpty(ArgumentMatchers.anyString())

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.DELETE,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV1_BadRequest() {
    givenBucket()

    val maxKeys = -1
    doThrow(S3Exception.INVALID_REQUEST_MAXKEYS).whenever(bucketService).verifyMaxKeys(maxKeys)
    val encodingtype = "not_valid"
    doThrow(S3Exception.INVALID_REQUEST_ENCODINGTYPE).whenever(bucketService).verifyEncodingType(encodingtype)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val maxKeysUri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.MAX_KEYS, maxKeys.toString())
      .build()
      .toString()
    val maxKeysResponse = restTemplate.exchange(
      maxKeysUri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(maxKeysResponse.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(maxKeysResponse.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_MAXKEYS)))

    val encodingTypeUri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.ENCODING_TYPE, encodingtype)
      .build()
      .toString()
    val encodingTypeResponse = restTemplate.exchange(
      encodingTypeUri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(encodingTypeResponse.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(encodingTypeResponse.body)
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_ENCODINGTYPE)))
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV2_BadRequest() {
    givenBucket()

    val maxKeys = -1
    doThrow(S3Exception.INVALID_REQUEST_MAXKEYS).whenever(bucketService).verifyMaxKeys(maxKeys)
    val encodingtype = "not_valid"
    doThrow(S3Exception.INVALID_REQUEST_ENCODINGTYPE).whenever(bucketService).verifyEncodingType(encodingtype)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val maxKeysUri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam("list-type", "2")
      .queryParam(AwsHttpParameters.MAX_KEYS, maxKeys.toString())
      .build().toString()
    val maxKeysResponse = restTemplate.exchange(
      maxKeysUri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(maxKeysResponse.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(maxKeysResponse.body)
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_MAXKEYS)))

    val encodingTypeUri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.ENCODING_TYPE, encodingtype)
      .queryParam("list-type", "2")
      .build().toString()
    val encodingTypeResponse = restTemplate.exchange(
      encodingTypeUri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(encodingTypeResponse.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(encodingTypeResponse.body)
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_ENCODINGTYPE)))
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV1_InternalServerError() {
    givenBucket()
    whenever(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
      .thenThrow(IllegalStateException("THIS IS EXPECTED"))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket/",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV2_InternalServerError() {
    givenBucket()
    whenever(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
      .thenThrow(IllegalStateException("THIS IS EXPECTED"))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/")
      .queryParam("list-type", "2")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV1_Ok() {
    givenBucket()
    val key = "key"
    val s3Object = bucketContents(key)
    val expected =
      ListBucketResult(
        TEST_BUCKET_NAME, null, null, 1000, false, null, null,
        listOf(s3Object), emptyList()
      )

    whenever(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
      .thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/test-bucket",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV2_Ok() {
    givenBucket()
    val key = "key"
    val s3Object = bucketContents(key)
    val expected =
      ListBucketResultV2(
        TEST_BUCKET_NAME, null, 1000, false,
        listOf(s3Object), emptyList(),
        null, null, null, null, null
      )

    whenever(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
      .thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam("list-type", "2")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  @Throws(Exception::class)
  fun testPutBucketObjectLockConfiguration_Ok() {
    givenBucket()
    val retention = DefaultRetention(1, null, Mode.COMPLIANCE)
    val rule = ObjectLockRule(retention)
    val expected = ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.OBJECT_LOCK, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(expected), headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)

    verify(bucketService).setObjectLockConfiguration(TEST_BUCKET_NAME, expected)
  }

  @Test
  @Throws(Exception::class)
  fun testGetBucketObjectLockConfiguration_Ok() {
    givenBucket()
    val retention = DefaultRetention(1, null, Mode.COMPLIANCE)
    val rule = ObjectLockRule(retention)
    val expected = ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule)

    whenever(bucketService.getObjectLockConfiguration(TEST_BUCKET_NAME)).thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.OBJECT_LOCK, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  @Throws(Exception::class)
  fun testPutBucketLifecycleConfiguration_Ok() {
    givenBucket()

    val filter1 = LifecycleRuleFilter(null, null, "documents/", null, null)
    val transition1 = Transition(null, 30, StorageClass.GLACIER)
    val rule1 = LifecycleRule(
      null, null, filter1, "id1", null, null,
      LifecycleRule.Status.ENABLED, listOf(transition1)
    )
    val filter2 = LifecycleRuleFilter(null, null, "logs/", null, null)
    val expiration2 = LifecycleExpiration(null, 365, null)
    val rule2 = LifecycleRule(
      null, expiration2, filter2, "id2", null, null,
      LifecycleRule.Status.ENABLED, null
    )
    val configuration = BucketLifecycleConfiguration(listOf(rule1, rule2))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.LIFECYCLE, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(configuration), headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)

    verify(bucketService).setBucketLifecycleConfiguration(TEST_BUCKET_NAME, configuration)
  }

  @Test
  @Throws(Exception::class)
  fun testGetBucketLifecycleConfiguration_Ok() {
    givenBucket()

    val filter1 = LifecycleRuleFilter(null, null, "documents/", null, null)
    val transition1 = Transition(null, 30, StorageClass.GLACIER)
    val rule1 = LifecycleRule(
      null, null, filter1, "id1", null, null,
      LifecycleRule.Status.ENABLED, listOf(transition1)
    )
    val filter2 = LifecycleRuleFilter(null, null, "logs/", null, null)
    val expiration2 = LifecycleExpiration(null, 365, null)
    val rule2 = LifecycleRule(
      null, expiration2, filter2, "id2", null, null,
      LifecycleRule.Status.ENABLED, null
    )
    val configuration = BucketLifecycleConfiguration(listOf(rule1, rule2))

    whenever(bucketService.getBucketLifecycleConfiguration(TEST_BUCKET_NAME)).thenReturn(configuration)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.LIFECYCLE, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(configuration))
  }

  private fun bucketContents(id: String): S3Object {
    return S3Object(
      id, "1234", "etag", "size", StorageClass.STANDARD, TEST_OWNER,
      ChecksumAlgorithm.SHA256
    )
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
  }

  private fun from(e: S3Exception): ErrorResponse {
    return ErrorResponse(
      e.code,
      e.message,
      null,
      null
    )
  }

  companion object {
    private val TEST_OWNER = Owner("123", "s3-mock-file-store")
    private const val TEST_BUCKET_NAME = "test-bucket"
    private val TEST_BUCKET = Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString())
  }
}
