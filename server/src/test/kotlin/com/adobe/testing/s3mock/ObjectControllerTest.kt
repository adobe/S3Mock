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

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.TagSet
import com.adobe.testing.s3mock.dto.Tagging
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.AwsHttpParameters
import com.adobe.testing.s3mock.util.DigestUtil
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.tuple.Pair
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyMap
import org.mockito.ArgumentMatchers.contains
import org.mockito.ArgumentMatchers.eq
import org.mockito.ArgumentMatchers.isNull
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.MockBeans
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.util.UriComponentsBuilder
import software.amazon.awssdk.core.checksums.Algorithm
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID

@MockBeans(
  MockBean(
    classes = [KmsKeyStore::class, MultipartService::class, BucketController::class, MultipartController::class]
  )
)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class ObjectControllerTest : BaseControllerTest() {
  @MockBean
  private lateinit var objectService: ObjectService

  @MockBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  @Throws(Exception::class)
  fun testPutObject_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile))
    val tempFile = Files.createTempFile("", "")
    FileUtils.copyFile(testFile, tempFile.toFile())
    whenever(
      objectService.toTempFile(
        any(
          InputStream::class.java
        ), any(HttpHeaders::class.java)
      )
    )
      .thenReturn(
        Pair.of(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), Algorithm.CRC32)
        )
      )

    whenever(
      objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE),
        anyMap(),
        any(Path::class.java),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, digest))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
    }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.PUT,
      HttpEntity(FileUtils.readFileToByteArray(testFile), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo("\"" + digest + "\"")
  }


  @Test
  @Throws(Exception::class)
  fun testPutObject_Options() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile))
    val tempFile = Files.createTempFile("", "")
    FileUtils.copyFile(testFile, tempFile.toFile())
    whenever(
      objectService.toTempFile(
        any(
          InputStream::class.java
        ), any(HttpHeaders::class.java)
      )
    )
      .thenReturn(
        Pair.of(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), Algorithm.CRC32)
        )
      )

    whenever(
      objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE),
        anyMap(),
        any(Path::class.java),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, digest))

    val optionsResponse = restTemplate.optionsForAllow("/test-bucket/$key")

    assertThat(optionsResponse).contains(HttpMethod.PUT)

    val origin = "http://www.someurl.com"
    val putHeaders = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
      this.origin = origin
    }

    val putResponse = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.PUT,
      HttpEntity(FileUtils.readFileToByteArray(testFile), putHeaders),
      String::class.java
    )

    assertThat(putResponse.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(putResponse.headers.eTag).isEqualTo("\"" + digest + "\"")
  }

  @Test
  @Throws(Exception::class)
  fun testPutObject_md5_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile))
    val tempFile = Files.createTempFile("", "")
    FileUtils.copyFile(testFile, tempFile.toFile())
    whenever(
      objectService.toTempFile(
        any(
          InputStream::class.java
        ), any(HttpHeaders::class.java)
      )
    )
      .thenReturn(
        Pair.of(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), Algorithm.CRC32)
        )
      )
    whenever(
      objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE),
        anyMap(),
        any(Path::class.java),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, hexDigest))

    val base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile))
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
      this[AwsHttpHeaders.CONTENT_MD5] = base64Digest
    }

    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.PUT,
      HttpEntity(FileUtils.readFileToByteArray(testFile), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo("\"" + hexDigest + "\"")
  }

  @Test
  @Throws(Exception::class)
  fun testPutObject_md5_BadRequest() {
    givenBucket()

    val testFile = File(UPLOAD_FILE_NAME)
    val base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile))

    whenever(
      objectService.toTempFile(
        any(
          InputStream::class.java
        ), any(HttpHeaders::class.java)
      )
    )
      .thenReturn(Pair.of(testFile.toPath(), "checksum"))
    Mockito.doThrow(S3Exception.BAD_REQUEST_MD5).`when`(objectService).verifyMd5(
      any(
        Path::class.java
      ), eq(base64Digest + 1)
    )

    val key = "sampleFile.txt"
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
      this[AwsHttpHeaders.CONTENT_MD5] = base64Digest + 1
    }

    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.PUT,
      HttpEntity(FileUtils.readFileToByteArray(testFile), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
  }

  @Test
  fun testGetObject_Encrypted_Ok() {
    givenBucket()
    val encryption = "aws:kms"
    val encryptionKey = "key-ref"
    val key = "name"
    val expectedS3ObjectMetadata = s3ObjectEncrypted(
      key, "digest",
      encryption, encryptionKey
    )

    whenever(objectService.verifyObjectExists(TEST_BUCKET_NAME, key))
      .thenReturn(expectedS3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
    }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      Void::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION]).containsExactly(encryption)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID])
      .containsExactly(encryptionKey)
  }

  @Test
  fun testHeadObject_Encrypted_Ok() {
    givenBucket()
    val encryption = "aws:kms"
    val encryptionKey = "key-ref"
    val key = "name"
    val expectedS3ObjectMetadata = s3ObjectEncrypted(
      key, "digest",
      encryption, encryptionKey
    )

    whenever(objectService.verifyObjectExists("test-bucket", key))
      .thenReturn(expectedS3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
    }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.HEAD,
      HttpEntity<Any>(headers),
      Void::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION]).containsExactly(encryption)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID])
      .containsExactly(encryptionKey)
  }

  @Test
  fun testHeadObject_NotFound() {
    givenBucket()
    val key = "name"

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
    }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.HEAD,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
  }

  @Test
  @Throws(JsonProcessingException::class)
  fun testGetObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner(
      "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
      "mtd@amazon.com"
    )
    val grantee = CanonicalUser(owner.id, owner.displayName, null, null)
    val policy = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )

    whenever(objectService.getAcl("test-bucket", key)).thenReturn(policy)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(policy))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner(
      "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
      "mtd@amazon.com"
    )
    val grantee = CanonicalUser(owner.id, owner.displayName, null, null)
    val policy = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(policy), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    verify(objectService).setAcl("test-bucket", key, policy)
  }

  @Test
  @Throws(Exception::class)
  fun testGetObjectTagging_Ok() {
    givenBucket()
    val key = "name"
    val tagging = Tagging(
      TagSet(
        listOf(
          Tag("key1", "value1"), Tag("key2", "value2")
        )
      )
    )
    val s3ObjectMetadata = s3ObjectMetadata(
      key, UUID.randomUUID().toString(),
      null, null, null, tagging.tagSet.tags
    )
    whenever(objectService.verifyObjectExists("test-bucket", key))
      .thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(tagging))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObjectTagging_Ok() {
    givenBucket()
    val key = "name"
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key))
      .thenReturn(s3ObjectMetadata)
    val tagging = Tagging(
      TagSet(
        listOf(
          Tag("key1", "value1"), Tag("key2", "value2")
        )
      )
    )

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(tagging), headers),
      String::class.java
    )

    verify(objectService).setObjectTags("test-bucket", key, tagging.tagSet.tags)
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  @Throws(Exception::class)
  fun testGetObjectRetention_Ok() {
    givenBucket()
    val key = "name"
    val instant = Instant.ofEpochMilli(1514477008120L)
    val retention = Retention(Mode.COMPLIANCE, instant)
    val s3ObjectMetadata = s3ObjectMetadata(
      key, UUID.randomUUID().toString(),
      null, null, retention, null
    )
    whenever(objectService.verifyObjectLockConfiguration("test-bucket", key))
      .thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(retention))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObjectRetention_Ok() {
    givenBucket()
    val key = "name"
    val instant = Instant.ofEpochMilli(1514477008120L)
    val retention = Retention(Mode.COMPLIANCE, instant)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder.fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored").build().toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(retention), headers),
      String::class.java
    )

    verify(objectService).setRetention("test-bucket", key, retention)
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
  }

  companion object {
    private const val TEST_BUCKET_NAME = "test-bucket"
    private val TEST_BUCKET = Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString())
    private const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"

    fun s3ObjectEncrypted(
      id: String?, digest: String?, encryption: String?, encryptionKey: String?
    ): S3ObjectMetadata {
      return s3ObjectMetadata(
        id, digest, encryption, encryptionKey, null, null
      )
    }

    @JvmOverloads
    fun s3ObjectMetadata(
      id: String?, digest: String?,
      encryption: String? = null, encryptionKey: String? = null,
      retention: Retention? = null, tags: List<Tag?>? = null
    ): S3ObjectMetadata {
      return S3ObjectMetadata(
        UUID.randomUUID(),
        id,
        "1234",
        "1234",
        digest,
        null,
        1L,
        Path.of(UPLOAD_FILE_NAME),
        null,
        tags,
        null,
        retention,
        null,
        null,
        encryptionHeaders(encryption, encryptionKey),
        null,
        null,
        StorageClass.STANDARD
      )
    }

    private fun encryptionHeaders(encryption: String?, encryptionKey: String?): Map<String, String?> {
      return mapOf(
        Pair(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION, encryption),
        Pair(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey)
      )
    }
  }
}


