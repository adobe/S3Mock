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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.AccessControlPolicy
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
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.AwsHttpParameters
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.isA
import org.mockito.kotlin.isNull
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.mock.web.MockMultipartFile
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.util.UriComponentsBuilder
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

@MockitoBean(types = [KmsKeyStore::class, MultipartService::class, BucketController::class, MultipartController::class])
@WebMvcTest(
  controllers = [ObjectController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"]
)
internal class ObjectControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  @Throws(Exception::class)
  fun testPutObject_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(testFile.inputStream())
    val tempFile = Files.createTempFile("testPutObject_Ok", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
    whenever(
      objectService.toTempFile(
        isA<InputStream>(),
        isA<HttpHeaders>()
      )
    )
      .thenReturn(
        Pair(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
        )
      )

    whenever(
      objectService.putObject(
        eq(TEST_BUCKET_NAME),
        eq(key),
        argThat<String>{ this.contains(MediaType.TEXT_PLAIN_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, digest))

    mockMvc.perform(
      put("/test-bucket/$key")
        .content(testFile.readBytes())
        .contentType(MediaType.TEXT_PLAIN)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"$digest\""))
  }


  @Test
  @Throws(Exception::class)
  fun testPutObject_withPathsegments_Ok() {
    givenBucket()
    val key = ".././sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(testFile.inputStream())
    val tempFile = Files.createTempFile("testPutObject_withPathsegments_Ok", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
    whenever(
      objectService.toTempFile(
        isA<InputStream>(),
        isA<HttpHeaders>()
      )
    )
      .thenReturn(
        tempFile to DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
      )

    whenever(
      objectService.putObject(
        eq(TEST_BUCKET_NAME),
        eq(key),
        argThat<String>{ this.contains(MediaType.TEXT_PLAIN_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, digest))

    mockMvc.perform(
      put("/test-bucket/$key")
        .content(testFile.readBytes())
        .contentType(MediaType.TEXT_PLAIN)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"$digest\""))
  }


  @Test
  @Throws(Exception::class)
  fun testPutObject_Options() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(testFile.inputStream())
    val tempFile = Files.createTempFile("testPutObject_Options", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
    whenever(
      objectService.toTempFile(
          isA<InputStream>(),
          isA<HttpHeaders>()
      )
    )
      .thenReturn(
        Pair(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
        )
      )

    whenever(
      objectService.putObject(
        eq(TEST_BUCKET_NAME),
        eq(key),
        argThat<String>{ this.contains(MediaType.TEXT_PLAIN_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, digest))

    mockMvc.perform(
      options("/test-bucket/$key")
    )
      .andExpect(status().isOk)
      .andExpect { result ->
        val allow = result.response.getHeader(HttpHeaders.ALLOW)
        assertThat(allow).isNotNull()
        assertThat(allow).contains("PUT")
      }

    val origin = "http://www.someurl.com"

    mockMvc.perform(
      put("/test-bucket/$key")
        .content(testFile.readBytes())
        .contentType(MediaType.TEXT_PLAIN)
        .accept(MediaType.APPLICATION_XML)
        .header(HttpHeaders.ORIGIN, origin)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"$digest\""))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObject_md5_Ok() {
    givenBucket()
    val key = "sampleFile.txt"

    val testFile = File(UPLOAD_FILE_NAME)
    val hexDigest = DigestUtil.hexDigest(testFile.inputStream())
    val tempFile = Files.createTempFile("testPutObject_md5_Ok", "").also {
      testFile.copyTo(it.toFile(), overwrite = true)
    }
    whenever(
      objectService.toTempFile(
        isA<InputStream>(),
        isA<HttpHeaders>()
      )
    )
      .thenReturn(
        Pair(
          tempFile,
          DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)
        )
      )
    whenever(
      objectService.putObject(
        eq(TEST_BUCKET_NAME),
        eq(key),
        argThat<String>{ this.contains(MediaType.TEXT_PLAIN_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata(key, hexDigest))

    val base64Digest = DigestUtil.base64Digest(testFile.inputStream())

    mockMvc.perform(
      put("/test-bucket/$key")
        .content(testFile.readBytes())
        .contentType(MediaType.TEXT_PLAIN)
        .accept(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.CONTENT_MD5, base64Digest)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"$hexDigest\""))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObject_md5_BadRequest() {
    givenBucket()

    val testFile = File(UPLOAD_FILE_NAME)
    val base64Digest = DigestUtil.base64Digest(testFile.inputStream())

    whenever(
      objectService.toTempFile(
        isA<InputStream>(),
        isA<HttpHeaders>()
      )
    )
      .thenReturn(Pair(testFile.toPath(), "checksum"))
    doThrow(S3Exception.BAD_REQUEST_MD5)
      .whenever(objectService)
      .verifyMd5(
        isA<Path>(),
        eq(base64Digest + 1)
      )

    val key = "sampleFile.txt"

    mockMvc.perform(
      put("/test-bucket/$key")
        .content(testFile.readBytes())
        .contentType(MediaType.TEXT_PLAIN)
        .accept(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.CONTENT_MD5, base64Digest + 1)
    )
      .andExpect(status().isBadRequest)
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

    mockMvc.perform(
      get("/test-bucket/$key")
        .accept(MediaType.ALL)
        .contentType(MediaType.TEXT_PLAIN)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey))
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

    mockMvc.perform(
      head("/test-bucket/$key")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.TEXT_PLAIN)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey))
  }

  @Test
  fun testHeadObject_NotFound() {
    givenBucket()
    val key = "name"
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenThrow(S3Exception.NO_SUCH_KEY)

    mockMvc.perform(
      head("/test-bucket/$key")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.TEXT_PLAIN)
    )
      .andExpect(status().isNotFound)
  }

  @Test
  fun testGetObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser(null, owner.id)
    val policy = AccessControlPolicy(
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL)),
      owner
    )
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    whenever(objectService.getAcl("test-bucket", key, null)).thenReturn(policy)

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(policy)))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser(null, owner.id)
    val policy = AccessControlPolicy(
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL)),
      owner
    )
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ACL, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(policy))
    )
      .andExpect(status().isOk)
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

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(tagging)))
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

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(tagging))
    )
      .andExpect(status().isOk)

    verify(objectService).setTags("test-bucket", key, null, tagging.tagSet.tags)
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

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(retention)))
  }

  @Test
  @Throws(Exception::class)
  fun testPutObjectRetention_Ok() {
    givenBucket()
    val key = "name"
    val instant = Instant.ofEpochMilli(1514477008120L)
    val retention = Retention(Mode.COMPLIANCE, instant)
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.RETENTION, "ignored")
      .build()
      .toString()
    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(retention))
    )
      .andExpect(status().isOk)

    verify(objectService).setRetention("test-bucket", key, null, retention)
  }

  @Test
  @Throws(Exception::class)
  fun testGetObject_Range_Ok() {
    givenBucket()
    val key = "sampleFile.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val digest = DigestUtil.hexDigest(testFile.inputStream())

    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata(key, digest))

    val total = testFile.length()

    mockMvc.perform(
      get("/test-bucket/$key")
        .accept(MediaType.ALL)
        .header("Range", "bytes=1-2")
    )
      .andExpect(status().isPartialContent)
      .andExpect(header().string(HttpHeaders.CONTENT_RANGE, "bytes 1-2/$total"))
      .andExpect(header().string(HttpHeaders.ACCEPT_RANGES, "bytes"))
      .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, 2))
      .andExpect(header().string(HttpHeaders.ETAG, "\"$digest\""))
  }

  @Test
  fun testDeleteObjectTagging_NoContent() {
    givenBucket()
    val key = "name"
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(s3ObjectMetadata)

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.TAGGING, "ignored")
      .build()
      .toString()

    mockMvc.perform(
      MockMvcRequestBuilders.delete(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNoContent)
    verify(objectService).setTags("test-bucket", key, null, null)
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

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.LEGAL_HOLD, "ignored")
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(legalHold)))
  }

  @Test
  fun testPutLegalHold_Ok() {
    givenBucket()
    val key = "locked"
    val legalHold = LegalHold(LegalHold.Status.OFF)
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.LEGAL_HOLD, "ignored")
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(legalHold))
    )
      .andExpect(status().isOk)
    verify(objectService).setLegalHold("test-bucket", key, null, legalHold)
  }

  @Test
  fun testGetObjectAttributes_Ok() {
    givenBucket()
    val key = "attrs.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val hex = DigestUtil.hexDigest(testFile.inputStream())
    val metadata = s3ObjectMetadata(key, hex)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(metadata)

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    val expected = GetObjectAttributesOutput(
      null,
      hex,
      null,
      testFile.length(),
      StorageClass.STANDARD
    )

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ETag,Checksum,ObjectSize,StorageClass")
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(expected)))
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
      listOf(
        DeletedS3Object(null, null, "a", "v1"),
        DeletedS3Object(null, null, "b", "v2")
      ),
      emptyList()
    )
    whenever(objectService.deleteObjects("test-bucket", body)).thenReturn(expected)

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket")
      .queryParam(AwsHttpParameters.DELETE, "ignored")
      .build()
      .toString()

    mockMvc.perform(
      MockMvcRequestBuilders.post(uri)
        .contentType(MediaType.APPLICATION_XML)
        .accept(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(body))
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(expected)))
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
      VersioningConfiguration.Status.ENABLED
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
      objectService.copyObject(
        eq(sourceBucket),
        eq(sourceKey),
        eq(sourceVersion),
        eq(targetBucket),
        eq(targetKey),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull()
      )
    ).thenReturn(copiedMeta)

    mockMvc.perform(
      put("/$targetBucket/$targetKey")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE, "REPLACE")
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE, "/$sourceBucket/$sourceKey?versionId=$sourceVersion")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_COPY_SOURCE_VERSION_ID, sourceVersion))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_VERSION_ID, "tv1"))
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
      objectService.copyObject(
        eq(sourceBucket),
        eq(sourceKey),
        isNull(),
        eq(targetBucket),
        eq(targetKey),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull()
      )
    ).thenReturn(null)

    mockMvc.perform(
      put("/$targetBucket/$targetKey")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE, "/$sourceBucket/$sourceKey")
    )
      .andExpect(status().isNotFound)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION, "aws:kms"))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, "kms-key"))
  }

  @Test
  fun testDeleteObject_Versioning_DeleteMarkerHeader() {
    val bucket = "test-bucket"
    val key = "to-delete.txt"

    // Bucket with versioning enabled
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED
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

    mockMvc.perform(
      MockMvcRequestBuilders.delete("/$bucket/$key")
    )
      .andExpect(status().isNoContent)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_DELETE_MARKER, "true"))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_VERSION_ID, "v1"))
  }

  @Test
  fun testPostObject_Ok_MinimalMultipart() {
    val bucket = "test-bucket"
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(TEST_BUCKETMETADATA)

    val key = "upload.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val tempFile = Files.createTempFile("postObject", "").also { testFile.copyTo(it.toFile(), overwrite = true) }

    // Single-arg overload used by postObject
    whenever(objectService.toTempFile(any<InputStream>()))
      .thenReturn(Pair(tempFile, DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)))

    val returned = s3ObjectMetadata(key, DigestUtil.hexDigest(testFile.inputStream()))
    whenever(
      objectService.putObject(
        eq(bucket),
        eq(key),
        argThat<String>{ this.contains(MediaType.APPLICATION_OCTET_STREAM_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.DEEP_ARCHIVE)
      )
    ).thenReturn(returned)


    mockMvc.perform(
      multipart("/$bucket")
        .file(MockMultipartFile("file", key, MediaType.APPLICATION_OCTET_STREAM_VALUE, testFile.readBytes()))
        .param("key", key)
        .param(X_AMZ_STORAGE_CLASS, StorageClass.DEEP_ARCHIVE.toString())
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(header().exists(HttpHeaders.ETAG))
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

    mockMvc.perform(
      get("/test-bucket/$key")
        .accept(MediaType.ALL)
        .header(AwsHttpHeaders.X_AMZ_CHECKSUM_MODE, "ENABLED")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32, "abcd1234"))
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

    mockMvc.perform(
      head(uri)
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.CONTENT_TYPE, contentType))
      .andExpect(header().string(HttpHeaders.CONTENT_DISPOSITION, contentDisposition))
  }

  @Test
  fun testGetObject_Range_Invalid_416() {
    givenBucket()
    val key = "rng.txt"
    val meta = s3ObjectMetadata(key)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(meta)

    mockMvc.perform(
      get("/test-bucket/$key")
        .header("Range", "bytes=9999999-10000000")
    )
      .andExpect(status().isRequestedRangeNotSatisfiable)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.INVALID_RANGE))))
  }

  @Test
  fun testPostObject_WithTaggingAndStorageClass() {
    val bucket = "test-bucket"
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(TEST_BUCKETMETADATA)

    val key = "upload-tags.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val tempFile = Files.createTempFile("postObjectTags", "").also { testFile.copyTo(it.toFile(), overwrite = true) }

    whenever(objectService.toTempFile(any<InputStream>()))
      .thenReturn(Pair(tempFile, DigestUtil.checksumFor(testFile.toPath(), DefaultChecksumAlgorithm.CRC32)))

    val tagging = Tagging(TagSet(listOf(Tag("k1", "v1"), Tag("k2", "v2"))))
    val returned = s3ObjectMetadata(key, DigestUtil.hexDigest(testFile.inputStream()))
    whenever(
      objectService.putObject(
        eq(bucket),
        eq(key),
        argThat<String>{ this.contains(MediaType.APPLICATION_OCTET_STREAM_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        argThat<List<Tag>> { this.containsAll(tagging.tagSet.tags) },
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(returned)

    mockMvc.perform(
      multipart("/$bucket")
        .file(MockMultipartFile("file", key, MediaType.APPLICATION_OCTET_STREAM_VALUE, testFile.readBytes()))
        .param("key", key)
        .param("tagging", MAPPER.writeValueAsString(tagging))
        .param("x-amz-storage-class", StorageClass.STANDARD.name)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(
        header().exists(HttpHeaders.ETAG))
    // verify storage class and tags were passed
    verify(objectService).putObject(
      eq(bucket),
      eq(key),
      argThat<String>{ this.contains(MediaType.APPLICATION_OCTET_STREAM_VALUE) },
      isA<Map<String, String>>(),
      isA<Path>(),
      isA<Map<String, String>>(),
      isA<Map<String, String>>(),
      argThat<List<Tag>> { this.containsAll(tagging.tagSet.tags) },
      isNull(),
      isNull(),
      eq(Owner.DEFAULT_OWNER),
      eq(StorageClass.STANDARD)
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
    whenever(objectService.toTempFile(any<InputStream>(), any<HttpHeaders>()))
      .thenReturn(Pair(temp, "crc32Value"))

    // Returned metadata should include checksum to be echoed as header
    val s3ObjectMetadata = s3ObjectMetadata(
      key,
      checksumAlgorithm = ChecksumAlgorithm.CRC32,
      checksum = "crc32Value",
    )

    whenever(
      objectService.putObject(
        eq(bucket),
        eq(key),
        argThat<String>{ this.contains(MediaType.APPLICATION_OCTET_STREAM_VALUE) },
        isA<Map<String, String>>(),
        isA<Path>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull(),
        eq(ChecksumAlgorithm.CRC32),
        eq("crc32Value"),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD)
      )
    ).thenReturn(s3ObjectMetadata)

    mockMvc.perform(
      put("/$bucket/$key")
        .content(src.readBytes())
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .header(HttpHeaders.IF_MATCH, '"' + "etag-123" + '"')
        .header(AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM, "CRC32")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32, "crc32Value"))
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_OBJECT_SIZE, s3ObjectMetadata.size))
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

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    val mvcResult = mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "Checksum,ObjectSize")
    )
      .andExpect(status().isOk)
      .andReturn()

    val got = MAPPER.readValue(mvcResult.response.contentAsString, GetObjectAttributesOutput::class.java)
    // only selected fields should be present
    assertThat(got.etag).isNull()
    assertThat(got.storageClass).isNull()
    assertThat(got.objectSize).isEqualTo(s3ObjectMetadata.dataPath.toFile().length())
    assertThat(got.checksum?.checksumCRC32C).isEqualTo("crcc-value")
    assertThat(got.checksum?.checksumType).isEqualTo(ChecksumType.FULL_OBJECT)
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
      objectService.copyObject(
        eq(sourceBucket),
        eq(sourceKey),
        isNull(),
        eq(targetBucket),
        eq(targetKey),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isA<Map<String, String>>(),
        isNull()
      )
    ).thenReturn(copied)

    mockMvc.perform(
      put("/$targetBucket/$targetKey")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE, "/$sourceBucket/$sourceKey")
        .header(AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE, "COPY")
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH, "\"etag-1\"")
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH, "\"etag-2\"")
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, Instant.now().toString())
        .header(AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, Instant.now().minusSeconds(60).toString())
    )
      .andExpect(status().isOk)

    // verify conditional headers reached the service verifier
    verify(objectService).verifyObjectMatchingForCopy(
      eq(listOf("\"etag-1\"")),
      eq(listOf("\"etag-2\"")),
      any(), // instants parsed to list
      any(),
      eq(srcMeta)
    )
    // verify copy called with COPY path (no user/store header replacements expected); we already set anyMap() above
    verify(objectService).copyObject(
      eq(sourceBucket),
      eq(sourceKey),
      isNull(),
      eq(targetBucket),
      eq(targetKey),
      isA<Map<String, String>>(),
      isA<Map<String, String>>(),
      isA<Map<String, String>>(),
      isNull()
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

    mockMvc.perform(
      MockMvcRequestBuilders.delete("/$bucket/$key")
        .header(AwsHttpHeaders.X_AMZ_IF_MATCH_LAST_MODIFIED_TIME, lm.toString())
        .header(AwsHttpHeaders.X_AMZ_IF_MATCH_SIZE, size.toString())
    )
      .andExpect(status().isNoContent)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_DELETE_MARKER, "false"))

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
      VersioningConfiguration.Status.ENABLED
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    val meta = s3ObjectMetadata(key, versionId = "v-123")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

   mockMvc.perform(
      head("/$bucket/$key")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_VERSION_ID, "v-123"))
      .andReturn()
  }

  @Test
  fun testGetObject_VersioningHeader_Present() {
    val bucket = "test-bucket"
    val key = "gv.txt"
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    val meta = s3ObjectMetadata(key, versionId = "v-9")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    mockMvc.perform(
      get("/$bucket/$key")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_VERSION_ID, "v-9"))
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

    mockMvc.perform(
      get("/$bucket/$key")
    )
      .andExpect(status().isOk)
      // store headers propagated
      .andExpect(header().string(HttpHeaders.CACHE_CONTROL, "max-age=3600"))
      .andExpect(header().string(HttpHeaders.CONTENT_LANGUAGE, "en"))
      // user metadata transformed to x-amz-meta-*
      .andExpect(header().string("x-amz-meta-foo", "bar"))
      .andExpect(header().string("x-amz-meta-answer", "42"))
  }

  @Test
  fun testGetObjectAttributes_EtagOnly_NoQuotes_AndVersionHeader() {
    val bucket = "test-bucket"
    val key = "attrs-etag.txt"
    val testFile = File(UPLOAD_FILE_NAME)

    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED
    )
    val versioningBucket = bucketMetadata(
      name = bucket,
      versioningConfiguration = versioningConfiguration
    )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    // note: S3ObjectMetadata normalizes etag to quoted; controller should strip quotes for attributes
    val hex = DigestUtil.hexDigest(testFile.inputStream())
    val meta = s3ObjectMetadata(key, hex, versionId = "va1")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    val uri = UriComponentsBuilder
      .fromUriString("/$bucket/$key")
      .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
        .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ETag")
    )
      .andExpect(status().isOk)
      // version header present
      .andExpect(header().string(AwsHttpHeaders.X_AMZ_VERSION_ID, "va1"))
      .andExpect { result ->
        val body = result.response.contentAsString
        // ETag must be without quotes in XML body
        assertThat(body).contains("<ETag>$hex</ETag>")
        // other fields not requested should not appear
        assertThat(body).doesNotContain("<ObjectSize>")
        assertThat(body).doesNotContain("<StorageClass>")
      }
  }

   private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.verifyBucketExists("test-bucket")).thenReturn(TEST_BUCKETMETADATA)
  }
}


