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

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Delete
import com.adobe.testing.s3mock.dto.DeleteResult
import com.adobe.testing.s3mock.dto.DeletedS3Object
import com.adobe.testing.s3mock.dto.GetObjectAttributesOutput
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.TagSet
import com.adobe.testing.s3mock.dto.Tagging
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.AwsHttpParameters
import com.adobe.testing.s3mock.util.DigestUtil
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.lang3.tuple.Pair
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyMap
import org.mockito.ArgumentMatchers.contains
import org.mockito.ArgumentMatchers.eq
import org.mockito.ArgumentMatchers.isNull
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.core.io.ByteArrayResource
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.util.UriComponentsBuilder
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID

@MockitoBean(types = [KmsKeyStore::class, MultipartService::class, BucketController::class, MultipartController::class])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class ObjectControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  @Throws(Exception::class)
  fun testPutObject_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))
    val tempFile = Files.createTempFile("testPutObject_Ok", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
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
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
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
      HttpEntity(testFile.readBytes(), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo("\"$digest\"")
  }


  @Test
  @Throws(Exception::class)
  fun testPutObject_Options() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))
    val tempFile = Files.createTempFile("testPutObject_Options", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
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
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
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
      HttpEntity(testFile.readBytes(), putHeaders),
      String::class.java
    )

    assertThat(putResponse.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(putResponse.headers.eTag).isEqualTo("\"$digest\"")
  }

  @Test
  @Throws(Exception::class)
  fun testPutObject_md5_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val hexDigest = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))
    val tempFile = Files.createTempFile("testPutObject_md5_Ok", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
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
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
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

    val base64Digest = DigestUtil.base64Digest(Files.newInputStream(testFile.toPath()))
    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.TEXT_PLAIN
      this[AwsHttpHeaders.CONTENT_MD5] = base64Digest
    }

    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.PUT,
      HttpEntity(testFile.readBytes(), headers),
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
    val base64Digest = DigestUtil.base64Digest(Files.newInputStream(testFile.toPath()))

    whenever(
      objectService.toTempFile(
        any(
          InputStream::class.java
        ), any(HttpHeaders::class.java)
      )
    )
      .thenReturn(Pair.of(testFile.toPath(), "checksum"))
    doThrow(S3Exception.BAD_REQUEST_MD5)
      .whenever(objectService)
      .verifyMd5(
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
      HttpEntity(testFile.readBytes(), headers),
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

    whenever(objectService.verifyObjectExists(TEST_BUCKET_NAME, key, null))
      .thenReturn(expectedS3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.ALL)
      this.contentType = MediaType.TEXT_PLAIN
    }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      ByteArray::class.java
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
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
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
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenThrow(S3Exception.NO_SUCH_KEY)

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
        "mtd@amazon.com",
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    val grantee = CanonicalUser(owner.displayName, owner.id)
    val policy = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )

    whenever(objectService.getAcl("test-bucket", key, null)).thenReturn(policy)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored")
      .build()
      .toString()
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
        "mtd@amazon.com",
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    val grantee = CanonicalUser(owner.displayName, owner.id)
    val policy = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(policy), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    verify(objectService).setAcl("test-bucket", key, null, policy)
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
      key,
      UUID.randomUUID().toString(),
      tags = tagging.tagSet.tags
    )
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()
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
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
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
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(tagging), headers),
      String::class.java
    )

    verify(objectService).setObjectTags("test-bucket", key, null, tagging.tagSet.tags)
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
      key,
      UUID.randomUUID().toString(),
      retention = retention,
    )
    whenever(objectService.verifyObjectLockConfiguration("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored")
      .build()
      .toString()
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
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored")
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(retention), headers),
      String::class.java
    )

    verify(objectService).setRetention("test-bucket", key, null, retention)
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  @Throws(Exception::class)
  fun testGetObject_Range_Ok() {
    givenBucket()
    val key = "sampleFile.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))

    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata(key, digest))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.ALL)
      this.set("Range", "bytes=1-2")
    }

    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      ByteArray::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.PARTIAL_CONTENT)
    val total = testFile.length()
    assertThat(response.headers.getFirst(HttpHeaders.CONTENT_RANGE)).isEqualTo("bytes 1-2/$total")
    assertThat(response.headers.getFirst(HttpHeaders.ACCEPT_RANGES)).isEqualTo("bytes")
    assertThat(response.headers.contentLength).isEqualTo(2)
    assertThat(response.headers.eTag).isEqualTo("\"$digest\"")
  }

  @Test
  fun testDeleteObjectTagging_NoContent() {
    givenBucket()
    val key = "name"
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.DELETE,
      HttpEntity<Any>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
    verify(objectService).setObjectTags("test-bucket", key, null, null)
  }

  @Test
  fun testGetLegalHold_Ok() {
    givenBucket()
    val key = "locked"
    val legalHold = LegalHold(LegalHold.Status.ON)
    val metadata = s3ObjectMetadata(
      key,
      UUID.randomUUID().toString(),
      legalHold = legalHold
    )
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(metadata)
    whenever(objectService.verifyObjectLockConfiguration("test-bucket", key, null)).thenReturn(metadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.LEGAL_HOLD, "ignored")
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(legalHold))
  }

  @Test
  fun testPutLegalHold_Ok() {
    givenBucket()
    val key = "locked"
    val legalHold = LegalHold(LegalHold.Status.OFF)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.LEGAL_HOLD, "ignored")
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity(MAPPER.writeValueAsString(legalHold), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    verify(objectService).setLegalHold("test-bucket", key, null, legalHold)
  }

  @Test
  fun testGetObjectAttributes_Ok() {
    givenBucket()
    val key = "attrs.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val hex = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))
    val metadata = s3ObjectMetadata(key, hex)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(metadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
      this.add(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ETag,Checksum,ObjectSize,StorageClass")
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )

    val expected = GetObjectAttributesOutput(
      null,
      hex,
      null,
      testFile.length(),
      StorageClass.STANDARD
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  fun testDeleteObjects_Ok() {
    givenBucket()
    val body = Delete(
      listOf(
        S3ObjectIdentifier("a", "etag", "0", "1", "v1"),
        S3ObjectIdentifier("b", "etag2", "0", "2", "v2")
      ),
      false
    )
    val expected = DeleteResult(
      emptyList(),
      listOf(
        DeletedS3Object(null, null, "a", "v1"),
        DeletedS3Object(null, null, "b", "v2")
      )
    )
    whenever(objectService.deleteObjects("test-bucket", body)).thenReturn(expected)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.DELETE, "ignored")
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(body), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(expected))
  }

  @Test
  fun testCopyObject_Ok_WithVersioningHeaders() {
    // Target and source buckets with versioning enabled
    val targetBucket = "test-bucket"
    val sourceBucket = "source-bucket"
    val sourceKey = "src.txt"
    val targetKey = "dst.txt"
    val sourceVersion = "sv1"

    // Configure buckets
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED,
      null
    )
    val versioningBucket = bucketMetadata(
      targetBucket,
      versioningConfiguration = versioningConfiguration,
    )
    val versioningSourceBucket = bucketMetadata(
      sourceBucket,
      versioningConfiguration = versioningConfiguration,
    )

    whenever(bucketService.verifyBucketExists(targetBucket)).thenReturn(versioningBucket)
    whenever(bucketService.verifyBucketExists(sourceBucket)).thenReturn(versioningSourceBucket)

    val srcMeta = s3ObjectMetadata(sourceKey, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists(sourceBucket, sourceKey, sourceVersion)).thenReturn(srcMeta)

    val copiedMeta = s3ObjectMetadata(
      targetKey,
        versionId = "tv1"
    )
    whenever(
      objectService.copyS3Object(
        eq(sourceBucket), eq(sourceKey), eq(sourceVersion),
        eq(targetBucket), eq(targetKey), anyMap(), anyMap(), anyMap(), isNull()
      )
    ).thenReturn(copiedMeta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
      // indicate REPLACE to test store/user headers path too (no specific headers asserted here)
      this[AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE] = "REPLACE"
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE] = "/$sourceBucket/$sourceKey?versionId=$sourceVersion"
    }

    val response = restTemplate.exchange(
      "/$targetBucket/$targetKey",
      HttpMethod.PUT,
      HttpEntity<Any>(null, headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    // Source version header must be present
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_COPY_SOURCE_VERSION_ID]).containsExactly(sourceVersion)
    // Target version header must be present (copy target version)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_VERSION_ID]).containsExactly("tv1")
  }

  @Test
  fun testCopyObject_NotFound_PropagatesEncryptionHeaders() {
    val targetBucket = "test-bucket"
    val sourceBucket = "source-bucket"
    val sourceKey = "src.txt"
    val targetKey = "dst.txt"

    // Buckets exist
    whenever(bucketService.verifyBucketExists(targetBucket)).thenReturn(TEST_BUCKETMETADATA)
    whenever(bucketService.verifyBucketExists(sourceBucket)).thenReturn(TEST_BUCKETMETADATA)

    // Source object exists with encryption headers
    val srcMeta = s3ObjectEncrypted(sourceKey, UUID.randomUUID().toString(), "aws:kms", "kms-key")
    whenever(objectService.verifyObjectExists(sourceBucket, sourceKey, null)).thenReturn(srcMeta)

    // Service indicates not found (e.g., filtered out) by returning null
    whenever(
      objectService.copyS3Object(
        eq(sourceBucket), eq(sourceKey), isNull(),
        eq(targetBucket), eq(targetKey), anyMap(), anyMap(), anyMap(), isNull()
      )
    ).thenReturn(null)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE] = "/$sourceBucket/$sourceKey"
    }

    val response = restTemplate.exchange(
      "/$targetBucket/$targetKey",
      HttpMethod.PUT,
      HttpEntity<Any>(null, headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION]).containsExactly("aws:kms")
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID]).containsExactly("kms-key")
  }

  @Test
  fun testDeleteObject_Versioning_DeleteMarkerHeader() {
    val bucket = "test-bucket"
    val key = "to-delete.txt"

    // Bucket with versioning enabled
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED,
      null
    )
    val versioningBucket = bucketMetadata(
      bucket,
      versioningConfiguration = versioningConfiguration,
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    val existingMeta = s3ObjectMetadata(
      key,
      versionId = "v1"
    )
    // First verify call returns the object
    whenever(objectService.verifyObjectExists(bucket, key, null))
      .thenReturn(existingMeta)
      // Second call after delete simulates a delete marker response
      .thenThrow(S3Exception.NO_SUCH_KEY_DELETE_MARKER)

    whenever(objectService.deleteObject(bucket, key, null)).thenReturn(true)

    val response = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.DELETE,
      HttpEntity<Void>(HttpHeaders()),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
    // Controller sets delete marker based on follow-up verify throwing NO_SUCH_KEY_DELETE_MARKER
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_DELETE_MARKER]).containsExactly("true")
    // When versioning enabled and original metadata had versionId, it should be echoed
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_VERSION_ID]).containsExactly("v1")
  }

  @Test
  fun testPostObject_Ok_MinimalMultipart() {
    val bucket = "test-bucket"
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(TEST_BUCKETMETADATA)

    val key = "upload.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val tempFile = Files.createTempFile("postObject", "").also { testFile.copyTo(it.toFile(), overwrite = true) }

    // Single-arg overload used by postObject
    whenever(objectService.toTempFile(any(InputStream::class.java)))
      .thenReturn(Pair.of(tempFile, DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)))

    val returned = s3ObjectMetadata(key, DigestUtil.hexDigest(Files.newInputStream(testFile.toPath())))
    whenever(
      objectService.putS3Object(
        eq(bucket), eq(key), any(), anyMap(), any(Path::class.java), anyMap(), anyMap(), isNull(), isNull(), isNull(), eq(Owner.DEFAULT_OWNER), isNull()
      )
    ).thenReturn(returned)

    // Build multipart request
    val fileResource = object : ByteArrayResource(testFile.readBytes()) {
      override fun getFilename(): String = key
    }
    val parts = LinkedMultiValueMap<String, Any>()
    parts.add("key", key)
    parts.add("file", HttpEntity(fileResource))

    val headers = HttpHeaders().apply { contentType = MediaType.MULTIPART_FORM_DATA }

    val response = restTemplate.postForEntity(
      "/$bucket",
      HttpEntity(parts, headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo(returned.etag)
  }

  @Test
  fun testGetObject_ChecksumHeaders_WhenEnabled() {
    givenBucket()
    val key = "chk.txt"
    val meta = s3ObjectMetadata(
      key,
      checksumAlgorithm = ChecksumAlgorithm.CRC32,
      checksum = "abcd1234"
    )

    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(meta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.ALL)
      this[AwsHttpHeaders.X_AMZ_CHECKSUM_MODE] = "ENABLED"
    }

    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      ByteArray::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32])
      .containsExactly("abcd1234")
  }

  @Test
  fun testHeadObject_OverrideHeaders_QueryParams() {
    givenBucket()
    val key = "ovr.txt"
    val meta = s3ObjectMetadata(key)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(meta)

    val contentDisposition = "attachment; filename=ovr.txt"
    val contentType = "text/html"
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("response-content-type", contentType)
      .queryParam("response-content-disposition", contentDisposition)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.HEAD,
      HttpEntity<Void>(HttpHeaders()),
      Void::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.contentType?.toString()).isEqualTo(contentType)
    assertThat(response.headers.getFirst(HttpHeaders.CONTENT_DISPOSITION)).isEqualTo(contentDisposition)
  }

  @Test
  fun testGetObject_Range_Invalid_416() {
    givenBucket()
    val key = "rng.txt"
    val meta = s3ObjectMetadata(key)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(meta)

    val headers = HttpHeaders().apply { this.set("Range", "bytes=9999999-10000000") }
    val response = restTemplate.exchange(
      "/test-bucket/$key",
      HttpMethod.GET,
      HttpEntity<Void>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
  }

  @Test
  fun testPostObject_WithTaggingAndStorageClass() {
    val bucket = "test-bucket"
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(TEST_BUCKETMETADATA)

    val key = "upload-tags.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val tempFile = Files.createTempFile("postObjectTags", "").also { testFile.copyTo(it.toFile(), overwrite = true) }

    whenever(objectService.toTempFile(any(InputStream::class.java)))
      .thenReturn(Pair.of(tempFile, DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)))

    val tagging = Tagging(TagSet(listOf(Tag("k1", "v1"), Tag("k2", "v2"))))
    val returned = s3ObjectMetadata(key, DigestUtil.hexDigest(Files.newInputStream(testFile.toPath())))
    whenever(
      objectService.putS3Object(
        eq(bucket), eq(key), any(), anyMap(), any(Path::class.java), anyMap(), anyMap(), any(), isNull(), isNull(), eq(Owner.DEFAULT_OWNER), eq(StorageClass.STANDARD)
      )
    ).thenReturn(returned)

    val fileResource = object : ByteArrayResource(testFile.readBytes()) { override fun getFilename(): String = key }
    val parts = LinkedMultiValueMap<String, Any>().apply {
      add("key", key)
      add("file", HttpEntity(fileResource))
      add("tagging", MAPPER.writeValueAsString(tagging))
      add("x-amz-storage-class", StorageClass.STANDARD.name)
    }

    val headers = HttpHeaders().apply { contentType = MediaType.MULTIPART_FORM_DATA }

    val response = restTemplate.postForEntity(
      "/$bucket",
      HttpEntity(parts, headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo(returned.etag)
    // verify storage class and tags were passed
    verify(objectService).putS3Object(
      eq(bucket), eq(key), any(), anyMap(), any(Path::class.java), anyMap(), anyMap(), eq(tagging.tagSet.tags), isNull(), isNull(), eq(Owner.DEFAULT_OWNER), eq(StorageClass.STANDARD)
    )
  }

  @Test
  fun testPutObject_WithIfMatch_AndSdkChecksum() {
    givenBucket()
    val bucket = "test-bucket"
    val key = "put-chksum.txt"
    val src = File(UPLOAD_FILE_NAME)
    val temp = Files.createTempFile("put-chk", "").also { src.copyTo(it.toFile(), overwrite = true) }

    // SDK checksum path: controller uses Right value from toTempFile
    whenever(objectService.toTempFile(any(InputStream::class.java), any(HttpHeaders::class.java)))
      .thenReturn(Pair.of(temp, "crc32Value"))

    // Returned metadata should include checksum to be echoed as header
    val s3ObjectMetadata = s3ObjectMetadata(
      key,
      checksumAlgorithm = ChecksumAlgorithm.CRC32,
      checksum = "crc32Value",
    )

    whenever(
      objectService.putS3Object(
        eq(bucket), eq(key), any(), anyMap(), any(Path::class.java), anyMap(), anyMap(), isNull(), eq(ChecksumAlgorithm.CRC32), eq("crc32Value"), eq(Owner.DEFAULT_OWNER), eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this[HttpHeaders.IF_MATCH] = listOf("\"etag-123\"")
      this[AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM] = "CRC32"
      contentType = MediaType.APPLICATION_OCTET_STREAM
    }

    val resp = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.PUT,
      HttpEntity(src.readBytes(), headers),
      String::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    // checksum header echoed from metadata
    assertThat(resp.headers[AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32]).containsExactly("crc32Value")
    // object size header present
    assertThat(resp.headers[AwsHttpHeaders.X_AMZ_OBJECT_SIZE]).containsExactly(s3ObjectMetadata.size())
    // verify matching path used and checksum verification invoked
    verify(objectService).verifyObjectMatching(eq(bucket), eq(key), any(), isNull())
    verify(objectService).verifyChecksum(eq(temp), eq("crc32Value"), eq(ChecksumAlgorithm.CRC32))
  }

  @Test
  fun testGetObjectAttributes_Selective_WithChecksum() {
    givenBucket()
    val key = "ga.txt"
    val s3ObjectMetadata = s3ObjectMetadata(
      key,
      checksumAlgorithm = ChecksumAlgorithm.CRC32C,
      checksum = "crcc-value",
    )

    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(s3ObjectMetadata)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this[AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES] = "Checksum,ObjectSize"
    }

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    val resp = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Void>(headers),
      String::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    val got = MAPPER.readValue(resp.body, GetObjectAttributesOutput::class.java)
    // only selected fields should be present
    assertThat(got.etag()).isNull()
    assertThat(got.storageClass()).isNull()
    assertThat(got.objectSize()).isEqualTo(s3ObjectMetadata.dataPath().toFile().length())
    assertThat(got.checksum().checksumCRC32C()).isEqualTo("crcc-value")
    assertThat(got.checksum().checksumType()).isEqualTo(ChecksumType.FULL_OBJECT)
  }

  @Test
  fun testCopyObject_MetadataDirectiveCopy_WithConditionalHeaders() {
    val targetBucket = "test-bucket"
    val sourceBucket = "src-bucket"
    val sourceKey = "a.txt"
    val targetKey = "b.txt"

    // Buckets exist (no versioning required for this test)
    whenever(bucketService.verifyBucketExists(targetBucket)).thenReturn(TEST_BUCKETMETADATA)
    whenever(bucketService.verifyBucketExists(sourceBucket)).thenReturn(TEST_BUCKETMETADATA)

    // Source object exists
    val srcMeta = s3ObjectMetadata(sourceKey)
    whenever(objectService.verifyObjectExists(sourceBucket, sourceKey, null)).thenReturn(srcMeta)

    // Copy returns metadata
    val copied = s3ObjectMetadata(targetKey)
    whenever(
      objectService.copyS3Object(
        eq(sourceBucket), eq(sourceKey), isNull(),
        eq(targetBucket), eq(targetKey), anyMap(), anyMap(), anyMap(), isNull()
      )
    ).thenReturn(copied)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE] = "/$sourceBucket/$sourceKey"
      this[AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE] = "COPY"
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH] = "\"etag-1\""
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH] = "\"etag-2\""
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE] = Instant.now().toString()
      this[AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE] = Instant.now().minusSeconds(60).toString()
    }

    val response = restTemplate.exchange(
      "/$targetBucket/$targetKey",
      HttpMethod.PUT,
      HttpEntity<Any>(null, headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    // verify conditional headers reached the service verifier
    verify(objectService).verifyObjectMatchingForCopy(
      eq(listOf("\"etag-1\"")),
      eq(listOf("\"etag-2\"")),
      any(), // instants parsed to list
      any(),
      eq(srcMeta)
    )
    // verify copy called with COPY path (no user/store header replacements expected); we already set anyMap() above
    verify(objectService).copyS3Object(
      eq(sourceBucket), eq(sourceKey), isNull(),
      eq(targetBucket), eq(targetKey), anyMap(), anyMap(), anyMap(), isNull()
    )
  }

  @Test
  fun testDeleteObject_MatchHeaders_DeletedFalse_InitialNoSuchKey() {
    val bucket = "test-bucket"
    val key = "to-del.txt"

    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(TEST_BUCKETMETADATA)

    // Initial verification throws NO_SUCH_KEY (controller should ignore and continue)
    doThrow(S3Exception.NO_SUCH_KEY).whenever(objectService).verifyObjectExists(bucket, key, null)
    // Deletion reports false
    whenever(objectService.deleteObject(bucket, key, null)).thenReturn(false)

    val lm = Instant.now()
    val size = 123L
    val headers = HttpHeaders().apply {
      this[AwsHttpHeaders.X_AMZ_IF_MATCH_LAST_MODIFIED_TIME] = lm.toString()
      this[AwsHttpHeaders.X_AMZ_IF_MATCH_SIZE] = size.toString()
    }

    val response = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.DELETE,
      HttpEntity<Void>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
    assertThat(response.headers[AwsHttpHeaders.X_AMZ_DELETE_MARKER]).containsExactly("false")

    // verify match headers forwarded with null metadata
    verify(objectService).verifyObjectMatching(
      isNull(),
      eq(listOf(lm)),
      eq(listOf(size)),
      isNull()
    )
  }

  @Test
  fun testHeadObject_VersioningHeader_Present() {
    val bucket = "test-bucket"
    val key = "vh.txt"
    // bucket with versioning enabled
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED,
      null
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    val meta = s3ObjectMetadata(key, versionId = "v-123")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    val resp = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.HEAD,
      HttpEntity<Void>(HttpHeaders()),
      Void::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(resp.headers[AwsHttpHeaders.X_AMZ_VERSION_ID]).containsExactly("v-123")
  }

  @Test
  fun testGetObject_VersioningHeader_Present() {
    val bucket = "test-bucket"
    val key = "gv.txt"
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED,
      null
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    val meta = s3ObjectMetadata(key, versionId = "v-9")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    val resp = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.GET,
      HttpEntity<Void>(HttpHeaders()),
      ByteArray::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(resp.headers[AwsHttpHeaders.X_AMZ_VERSION_ID]).containsExactly("v-9")
  }

  @Test
  fun testGetObject_PropagatesStoreAndUserHeaders() {
    givenBucket()
    val bucket = "test-bucket"
    val key = "hdrs.txt"

    // Build metadata with store headers and user metadata
    val storeHeaders = mapOf(
      HttpHeaders.CACHE_CONTROL to "max-age=3600",
      HttpHeaders.CONTENT_LANGUAGE to "en"
    )
    val userMeta = mapOf(
      "foo" to "bar",
      "answer" to "42"
    )

    val s3ObjectMetadata = s3ObjectMetadata(
      key,
      userMetadata = userMeta,
      storeHeaders = storeHeaders,
    )

    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(s3ObjectMetadata)

    val resp = restTemplate.exchange(
      "/$bucket/$key",
      HttpMethod.GET,
      HttpEntity<Void>(HttpHeaders()),
      ByteArray::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    // store headers propagated
    assertThat(resp.headers.getFirst(HttpHeaders.CACHE_CONTROL)).isEqualTo("max-age=3600")
    assertThat(resp.headers.getFirst(HttpHeaders.CONTENT_LANGUAGE)).isEqualTo("en")
    // user metadata transformed to x-amz-meta-*
    assertThat(resp.headers.getFirst("x-amz-meta-foo")).isEqualTo("bar")
    assertThat(resp.headers.getFirst("x-amz-meta-answer")).isEqualTo("42")
  }

  @Test
  fun testGetObjectAttributes_EtagOnly_NoQuotes_AndVersionHeader() {
    val bucket = "test-bucket"
    val key = "attrs-etag.txt"
    val testFile = File(UPLOAD_FILE_NAME)

    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED,
      null
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    // note: S3ObjectMetadata normalizes etag to quoted; controller should strip quotes for attributes
    val hex = DigestUtil.hexDigest(Files.newInputStream(testFile.toPath()))
    val meta = s3ObjectMetadata(key, hex, versionId = "va1")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this[AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES] = "ETag"
    }
    val uri = UriComponentsBuilder
      .fromUriString("/$bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    val resp = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity<Void>(headers),
      String::class.java
    )

    assertThat(resp.statusCode).isEqualTo(HttpStatus.OK)
    // version header present
    assertThat(resp.headers[AwsHttpHeaders.X_AMZ_VERSION_ID]).containsExactly("va1")
    // ETag must be without quotes in XML body
    assertThat(resp.body).contains("<ETag>$hex</ETag>")
    // other fields not requested should not appear
    assertThat(resp.body).doesNotContain("<ObjectSize>")
    assertThat(resp.body).doesNotContain("<StorageClass>")
  }

   private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.verifyBucketExists("test-bucket")).thenReturn(TEST_BUCKETMETADATA)
  }
}


