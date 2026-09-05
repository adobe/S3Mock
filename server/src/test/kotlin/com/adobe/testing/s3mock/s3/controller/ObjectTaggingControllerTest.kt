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
package com.adobe.testing.s3mock.s3.controller

import com.adobe.testing.s3mock.s3.dto.Tag
import com.adobe.testing.s3mock.s3.dto.TagSet
import com.adobe.testing.s3mock.s3.dto.Tagging
import com.adobe.testing.s3mock.s3.service.BucketService
import com.adobe.testing.s3mock.s3.service.MultipartService
import com.adobe.testing.s3mock.s3.service.ObjectService
import com.adobe.testing.s3mock.s3.store.KmsKeyStore
import com.adobe.testing.s3mock.s3.util.AwsHttpParameters
import org.junit.jupiter.api.Test
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.util.UriComponentsBuilder
import java.util.UUID

@MockitoBean(
  types = [
    KmsKeyStore::class,
    MultipartService::class,
    BucketController::class,
    MultipartController::class,
    ObjectController::class,
    ObjectAclController::class,
    ObjectLegalHoldController::class,
    ObjectRetentionController::class,
    ObjectAttributesController::class,
  ],
)
@WebMvcTest(
  controllers = [ObjectTaggingController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"],
)
internal class ObjectTaggingControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testGetObjectTagging_Ok() {
    givenBucket()
    val key = "name"
    val tagging =
      Tagging(
        TagSet(
          listOf(
            Tag("key1", "value1"),
            Tag("key2", "value2"),
          ),
        ),
      )
    val s3ObjectMetadata =
      s3ObjectMetadata(
        key,
        UUID.randomUUID().toString(),
        tags = tagging.tagSet.tags,
      )
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.TAGGING, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML),
      ).andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(tagging)))
  }

  @Test
  fun testGetObjectTagging_NoTags_ReturnsEmptyTagSet() {
    givenBucket()
    val key = "name"
    // No tags were ever set on the object: S3 answers with a Tagging document
    // carrying an empty TagSet, not with an empty body.
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.TAGGING, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML),
      ).andExpect(status().isOk)
      // Assert the literal wire format instead of round-tripping through the same
      // mapper the controller uses: the point of this test is that <TagSet> is
      // present in the response body even when there is nothing to report.
      .andExpect(
        content().string(
          """<?xml version="1.0" encoding="UTF-8"?>""" +
            """<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet/></Tagging>""",
        ),
      )
  }

  @Test
  fun testPutObjectTagging_Ok() {
    givenBucket()
    val key = "name"
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    val tagging =
      Tagging(
        TagSet(
          listOf(
            Tag("key1", "value1"),
            Tag("key2", "value2"),
          ),
        ),
      )

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.TAGGING, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        put(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML)
          .content(MAPPER.writeValueAsString(tagging)),
      ).andExpect(status().isOk)

    verify(objectService).setTags("test-bucket", key, null, tagging.tagSet.tags)
  }

  @Test
  fun testDeleteObjectTagging_NoContent() {
    givenBucket()
    val key = "name"
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null)).thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.TAGGING, "ignored")
        .build()
        .toString()

    mockMvc
      .perform(
        MockMvcRequestBuilders
          .delete(uri)
          .accept(MediaType.APPLICATION_XML),
      ).andExpect(status().isNoContent)
    verify(objectService).setTags("test-bucket", key, null, null)
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.verifyBucketExists("test-bucket")).thenReturn(TEST_BUCKETMETADATA)
  }
}
