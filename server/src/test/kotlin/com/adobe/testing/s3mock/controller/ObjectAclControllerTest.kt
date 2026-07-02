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

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.Owner
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
import java.util.UUID

@MockitoBean(
  types = [
    KmsKeyStore::class,
    MultipartService::class,
    BucketController::class,
    MultipartController::class,
    ObjectController::class,
    ObjectTaggingController::class,
    ObjectLegalHoldController::class,
    ObjectRetentionController::class,
    ObjectAttributesController::class,
  ],
)
@WebMvcTest(
  controllers = [ObjectAclController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"],
)
internal class ObjectAclControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var objectService: ObjectService

  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testGetObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser(null, owner.id)
    val policy =
      AccessControlPolicy(
        listOf(Grant(grantee, Grant.Permission.FULL_CONTROL)),
        owner,
      )
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)
    whenever(objectService.getAcl("test-bucket", key, null)).thenReturn(policy)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.ACL, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        get(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML),
      ).andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(policy)))
  }

  @Test
  fun testPutObjectAcl_Ok() {
    givenBucket()
    val key = "name"

    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser(null, owner.id)
    val policy =
      AccessControlPolicy(
        listOf(Grant(grantee, Grant.Permission.FULL_CONTROL)),
        owner,
      )
    val s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists("test-bucket", key, null))
      .thenReturn(s3ObjectMetadata)

    val uri =
      UriComponentsBuilder
        .fromUriString("/test-bucket/$key")
        .queryParam(AwsHttpParameters.ACL, "ignored")
        .build()
        .toString()
    mockMvc
      .perform(
        put(uri)
          .accept(MediaType.APPLICATION_XML)
          .contentType(MediaType.APPLICATION_XML)
          .content(MAPPER.writeValueAsString(policy)),
      ).andExpect(status().isOk)
    verify(objectService).setAcl("test-bucket", key, null, policy)
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
    whenever(bucketService.verifyBucketExists("test-bucket")).thenReturn(TEST_BUCKETMETADATA)
  }
}
