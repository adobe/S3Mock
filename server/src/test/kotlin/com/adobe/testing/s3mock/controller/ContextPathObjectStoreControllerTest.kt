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

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.Buckets
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.MultipartStore
import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.nio.file.Paths
import java.time.Instant

@MockitoBean(types = [KmsKeyStore::class, ObjectService::class, MultipartService::class, MultipartStore::class])
@WebMvcTest(
  controllers = [ObjectController::class],
  properties = ["com.adobe.testing.s3mock.controller.contextPath=s3-mock"]
)
internal class ContextPathObjectStoreControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var bucketService: BucketService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testListBuckets_Ok() {
    val bucketList = listOf(
      TEST_BUCKET,
      Bucket("testBucket1", "us-east-1", Instant.now().toString(), Paths.get("/tmp/foo/2"))
    )

    val expected = ListAllMyBucketsResult(TEST_OWNER, Buckets(bucketList), null, null)
    whenever(
      bucketService.listBuckets(
        null,
        null,
        1000,
        null
      )
    ).thenReturn(expected)

    mockMvc.perform(
      get("/s3-mock/")
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(expected)))
  }
}


