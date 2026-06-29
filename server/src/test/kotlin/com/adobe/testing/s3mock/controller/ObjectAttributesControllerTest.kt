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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.GetObjectAttributesOutput
import com.adobe.testing.s3mock.dto.ObjectPart
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.AwsHttpParameters
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.util.UriComponentsBuilder
import java.io.File
import java.util.UUID

@MockitoBean(
  types = [
    KmsKeyStore::class,
    MultipartService::class,
    BucketController::class,
    MultipartController::class,
    ObjectController::class,
    ObjectAclController::class,
    ObjectTaggingController::class,
    ObjectLegalHoldController::class,
    ObjectRetentionController::class,
  ],
)
@WebMvcTest(
  controllers = [ObjectAttributesController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"],
)
internal class ObjectAttributesControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testGetObjectAttributes_Ok() {
    givenBucket()
    val key = "attrs.txt"
    val testFile = File(UPLOAD_FILE_NAME)
    val hex = DigestUtil.hexDigest(testFile.inputStream())
    val metadata = s3ObjectMetadata(key, hex)
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(metadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
        .build()
        .toString()

    val expected =
      GetObjectAttributesOutput(
        null,
        hex,
        null,
        testFile.length(),
        StorageClass.STANDARD,
      )

    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML)
          .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ETag,Checksum,ObjectSize,StorageClass"),
      ).andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(expected)))
  }

  @Test
  fun testGetObjectAttributes_Selective_WithChecksum() {
    givenBucket()
    val key = "ga.txt"
    val s3ObjectMetadata =
      s3ObjectMetadata(
        key,
        checksumAlgorithm = ChecksumAlgorithm.CRC32C,
        checksum = "crcc-value",
      )

    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
        .build()
        .toString()

    val mvcResult =
      mockMvc
        .perform(
          get(uri)
            .accept(MediaType.APPLICATION_XML)
            .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "Checksum,ObjectSize"),
        ).andExpect(status().isOk)
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
  fun testGetObjectAttributes_WithObjectParts() {
    givenBucket()
    val key = "multipart.txt"
    val part = ObjectPart(checksumCRC32 = "abc123==", partNumber = 1, size = 1024L)
    val metadata = s3ObjectMetadata(key).copy(parts = listOf(part))
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(metadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
        .build()
        .toString()

    val mvcResult =
      mockMvc
        .perform(
          get(uri)
            .accept(MediaType.APPLICATION_XML)
            .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ObjectParts"),
        ).andExpect(status().isOk)
        .andReturn()

    val got = MAPPER.readValue(mvcResult.response.contentAsString, GetObjectAttributesOutput::class.java)
    assertThat(got.objectParts).isNotNull().hasSize(1)
    val objectParts = got.objectParts!![0]
    assertThat(objectParts.partsCount).isEqualTo(1)
    assertThat(objectParts.parts).hasSize(1)
    assertThat(objectParts.parts!![0].partNumber).isEqualTo(1)
    assertThat(objectParts.parts!![0].size).isEqualTo(1024L)
    assertThat(objectParts.parts!![0].checksumCRC32).isEqualTo("abc123==")
    // attributes not requested must be absent
    assertThat(got.etag).isNull()
    assertThat(got.storageClass).isNull()
  }

  @Test
  fun testGetObjectAttributes_EtagOnly_NoQuotes_AndVersionHeader() {
    val bucket = "test-bucket"
    val key = "attrs-etag.txt"
    val testFile = File(UPLOAD_FILE_NAME)

    val versioningConfiguration =
      VersioningConfiguration(
        VersioningConfiguration.MFADelete.DISABLED,
        VersioningConfiguration.Status.ENABLED,
      )
    val versioningBucket =
      bucketMetadata(
        name = bucket,
        versioningConfiguration = versioningConfiguration,
      )
    whenever(bucketService.verifyBucketExists(bucket)).thenReturn(versioningBucket)

    // note: S3ObjectMetadata normalizes etag to quoted; controller should strip quotes for attributes
    val hex = DigestUtil.hexDigest(testFile.inputStream())
    val meta = s3ObjectMetadata(key, hex, versionId = "va1")
    whenever(objectService.verifyObjectExists(bucket, key, null)).thenReturn(meta)

    val uri =
      UriComponentsBuilder
        .fromUriString("/$bucket/$key")
        .queryParam(AwsHttpParameters.ATTRIBUTES, "ignored")
        .build()
        .toString()

    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .header(AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES, "ETag"),
      ).andExpect(status().isOk)
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
