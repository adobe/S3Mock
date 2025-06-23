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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.BucketType.DIRECTORY
import com.adobe.testing.s3mock.dto.Buckets
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CreateBucketConfiguration
import com.adobe.testing.s3mock.dto.DataRedundancy.SINGLE_AVAILABILITY_ZONE
import com.adobe.testing.s3mock.dto.DefaultRetention
import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.dto.LifecycleExpiration
import com.adobe.testing.s3mock.dto.LifecycleRule
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.dto.ListBucketResult
import com.adobe.testing.s3mock.dto.ListBucketResultV2
import com.adobe.testing.s3mock.dto.LocationConstraint
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.LocationType.AVAILABILITY_ZONE
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.ObjectLockRule
import com.adobe.testing.s3mock.dto.ObjectOwnership.BUCKET_OWNER_ENFORCED
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Region
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Transition
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_NAME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_TYPE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_REGION
import com.adobe.testing.s3mock.util.AwsHttpParameters
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
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
import java.net.URI
import java.nio.file.Paths
import java.time.Instant

@MockBean(
  classes = [KmsKeyStore::class, ObjectService::class, MultipartService::class, ObjectController::class, MultipartController::class]
)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = ["com.adobe.testing.s3mock.region=us-east-1"])
internal class BucketControllerTest : BaseControllerTest() {
  @MockBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  fun `HEAD bucket returns OK if bucket exists`() {
    givenBucket()
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
  fun `HEAD bucket returns bucketInfo and locationInfo headers if available`() {
    whenever(bucketService.bucketLocationHeaders(any(BucketMetadata::class.java))).thenCallRealMethod()
    givenBucket(bucketMetadata(
      BucketInfo(SINGLE_AVAILABILITY_ZONE, DIRECTORY),
      LocationInfo("SomeName", AVAILABILITY_ZONE)
    ))
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
    assertThat(response.headers[X_AMZ_BUCKET_LOCATION_TYPE]).isEqualTo(listOf(AVAILABILITY_ZONE.toString()))
    assertThat(response.headers[X_AMZ_BUCKET_LOCATION_NAME]).isEqualTo(listOf("SomeName"))
    assertThat(response.headers[X_AMZ_BUCKET_REGION]).isEqualTo(listOf(BUCKET_REGION))
  }

  @Test
  fun `HEAD bucket for non-existing bucket returns 404`() {
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
  fun `creating a bucket without configuration returns OK and location`() {
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val response = restTemplate.exchange(
      "/${TEST_BUCKET_NAME}",
      HttpMethod.PUT,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.location).isEqualTo(URI.create("/${TEST_BUCKET_NAME}"))
    verify(bucketService).createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED, null, null, null)
  }

  @Test
  fun `PUT bucket with configuration returns OK and location`() {
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val bucketInfo = BucketInfo(SINGLE_AVAILABILITY_ZONE, DIRECTORY)
    val locationInfo = LocationInfo("SomeName", AVAILABILITY_ZONE)
    val createBucketConfiguration = CreateBucketConfiguration(
      bucketInfo,
      locationInfo,
      LocationConstraint(BUCKET_REGION),
    )

    val response = restTemplate.exchange(
      "/${TEST_BUCKET_NAME}",
      HttpMethod.PUT,
      HttpEntity<CreateBucketConfiguration>(createBucketConfiguration, headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.location).isEqualTo(URI.create("/${TEST_BUCKET_NAME}"))
    verify(bucketService).createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED, BUCKET_REGION, bucketInfo, locationInfo)
  }

  @Test
  fun `PUT bucket returns InternalServerError if bucket can't be persisted`() {
    whenever(bucketService.createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED, null, null, null))
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
            null, null, null, null, null, null, null, null, null,
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
  fun `GET list buckets returns all buckets if no parameters are given`() {
    val expected = givenBuckets(2)

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
  fun `GET list buckets forwards all parameters to BucketService#listBuckets`() {
    val prefix = "prefix"
    val continuationToken = "continuationToken"
    val region = Region.EU_CENTRAL_1
    val maxBuckets = 10
    val expected = givenBuckets(2,
      prefix,
      continuationToken,
      region,
      maxBuckets
    )

    val uri = UriComponentsBuilder
      .fromUriString("/")
      .queryParam(AwsHttpParameters.PREFIX, prefix)
      .queryParam(AwsHttpParameters.BUCKET_REGION, region.toString())
      .queryParam(AwsHttpParameters.CONTINUATION_TOKEN, continuationToken)
      .queryParam(AwsHttpParameters.MAX_BUCKETS, maxBuckets.toString())
      .build()
      .toString()
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
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
  fun `GET list buckets result is empty if no buckets exist`() {
    val expected = givenBuckets(0)

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
    assertThat(expected.buckets.buckets).isEmpty()
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV1_BadRequest() {
    givenBucket()

    val maxKeys = -1
    doThrow(S3Exception.INVALID_REQUEST_MAX_KEYS).whenever(bucketService).verifyMaxKeys(maxKeys)
    val encodingtype = "not_valid"
    doThrow(S3Exception.INVALID_REQUEST_ENCODING_TYPE).whenever(bucketService).verifyEncodingType(encodingtype)

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
    assertThat(maxKeysResponse.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_MAX_KEYS)))

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
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_ENCODING_TYPE)))
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV2_BadRequest() {
    givenBucket()

    val maxKeys = -1
    doThrow(S3Exception.INVALID_REQUEST_MAX_KEYS).whenever(bucketService).verifyMaxKeys(maxKeys)
    val encodingtype = "not_valid"
    doThrow(S3Exception.INVALID_REQUEST_ENCODING_TYPE).whenever(bucketService).verifyEncodingType(encodingtype)

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
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_MAX_KEYS)))

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
      .isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_REQUEST_ENCODING_TYPE)))
  }

  @Test
  @Throws(Exception::class)
  fun testListObjectsV1_InternalServerError() {
    givenBucket()
    whenever(
      bucketService.listObjectsV1(
        TEST_BUCKET_NAME,
        null,
        null,
        null,
        null,
        MAX_KEYS_DEFAULT
      )
    ).thenThrow(IllegalStateException("THIS IS EXPECTED"))

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
    whenever(
      bucketService.listObjectsV2(
        TEST_BUCKET_NAME,
        null,
        null,
        null,
        null,
        MAX_KEYS_DEFAULT,
        null,
        false
      )
    ).thenThrow(IllegalStateException("THIS IS EXPECTED"))

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
        emptyList(),
        listOf(s3Object),
        null,
        null,
        false,
        null,
        MAX_KEYS_DEFAULT,
        TEST_BUCKET_NAME,
        null,
        null
      )

    whenever(
      bucketService.listObjectsV1(
        TEST_BUCKET_NAME,
        null,
        null,
        null,
        null,
        MAX_KEYS_DEFAULT
      )
    ).thenReturn(expected)

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
        emptyList(),
        listOf(s3Object),
        null,
        null,
        null,
        false,
        MAX_KEYS_DEFAULT,
        TEST_BUCKET_NAME,
        null,
        null,
        null,
        null
      )

    whenever(
      bucketService.listObjectsV2(
        TEST_BUCKET_NAME,
        null,
        null,
        null,
        null,
        MAX_KEYS_DEFAULT,
        null,
        false
      )
    ).thenReturn(expected)

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
    val retention = DefaultRetention(1, Mode.COMPLIANCE, null)
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
    val retention = DefaultRetention(1, Mode.COMPLIANCE, null)
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


  private fun givenBuckets(count: Int = 0,
       prefix: String? = null,
       continuationToken: String? = null,
       region: Region? = null,
       maxBuckets: Int = MAX_BUCKETS_DEFAULT): ListAllMyBucketsResult {

    val namePrefix = "test-bucket"
    val bucketList = mutableListOf<Bucket>()

    for(i in 0 until count) {
      val bucket = Bucket(
        "$namePrefix-$i",
        BUCKET_REGION,
        Instant.now().toString(),
        Paths.get("/tmp/foo/$i")
      )
      bucketList.add(bucket)
    }

    val expected = ListAllMyBucketsResult(
      TEST_OWNER,
      Buckets(bucketList),
      prefix,
      continuationToken
    )
    whenever(bucketService.listBuckets(
      region,
      continuationToken,
      maxBuckets,
      prefix)
    ).thenReturn(expected)

    return expected
  }

  private fun bucketContents(id: String): S3Object {
    return S3Object(
      ChecksumAlgorithm.SHA256,
      ChecksumType.FULL_OBJECT,
      "etag",
      id,
      "1234",
      TEST_OWNER,
      null,
      "size",
      StorageClass.STANDARD
    )
  }

  private fun givenBucket(bucketMetadata: BucketMetadata = bucketMetadata()) {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMetadata)
  }

  private fun from(e: S3Exception): ErrorResponse {
    return ErrorResponse(
      e.code,
      e.message,
      null,
      null
    )
  }

  private fun bucketMetadata(bucketInfo: BucketInfo? = null,
      locationInfo: LocationInfo? = null): BucketMetadata {
    return BucketMetadata(
      TEST_BUCKET_NAME,
      CREATION_DATE,
      null,
      null,
      null,
      null,
      BUCKET_PATH,
      BUCKET_REGION,
      bucketInfo,
      locationInfo,
    )
  }

  companion object {
    private val TEST_OWNER = Owner("s3-mock-file-store", "123")
    private const val TEST_BUCKET_NAME = "test-bucket"
    private val CREATION_DATE = Instant.now().toString()
    private const val BUCKET_REGION = "us-east-1"
    private val BUCKET_PATH = Paths.get("/tmp/foo/1")
    private const val MAX_BUCKETS_DEFAULT = 1000
    private const val MAX_KEYS_DEFAULT = 1000

    private val TEST_BUCKET = Bucket(
      TEST_BUCKET_NAME,
      BUCKET_REGION,
      CREATION_DATE,
      BUCKET_PATH
    )
  }
}
