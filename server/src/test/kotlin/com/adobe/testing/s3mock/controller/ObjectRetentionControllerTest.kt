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

import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpParameters
import org.junit.jupiter.api.Test
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.util.UriComponentsBuilder
import java.time.Instant
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
    ObjectAttributesController::class,
  ],
)
@WebMvcTest(
  controllers = [ObjectRetentionController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"],
)
internal class ObjectRetentionControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testGetObjectRetention_Ok() {
    givenBucket()
    val key = "name"
    val instant = Instant.ofEpochMilli(1514477008120L)
    val retention = Retention(Mode.COMPLIANCE, instant)
    val s3ObjectMetadata =
      s3ObjectMetadata(
        key,
        UUID.randomUUID().toString(),
        retention = retention,
      )
    whenever(objectService.verifyRetentionExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.RETENTION, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML),
      ).andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(retention)))
  }

  @Test
  fun testPutObjectRetention_Ok() {
    givenBucket()
    val key = "name"
    val instant = Instant.ofEpochMilli(1514477008120L)
    val retention = Retention(Mode.COMPLIANCE, instant)
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.RETENTION, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        put(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML)
          .content(MAPPER.writeValueAsString(retention)),
      ).andExpect(status().isOk)

    verify(objectService).setRetention("test-bucket", key, null, retention)
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.verifyBucketExists("test-bucket")).thenReturn(TEST_BUCKETMETADATA)
  }
}
