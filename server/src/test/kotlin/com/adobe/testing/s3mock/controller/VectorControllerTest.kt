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

import com.adobe.testing.s3mock.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.service.VectorService
import com.adobe.testing.s3mock.store.KmsKeyStore
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@MockitoBean(
  types = [KmsKeyStore::class, BucketService::class, ObjectService::class, MultipartService::class],
)
@WebMvcTest(controllers = [VectorController::class])
internal class VectorControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var vectorService: VectorService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun `create vector bucket returns JSON response`() {
    whenever(vectorService.createVectorBucket(any())).thenReturn(
      CreateVectorBucketResponse("arn:aws:s3vectors:us-east-1:123456789012:bucket/vector-bucket"),
    )

    mockMvc
      .perform(
        post("/CreateVectorBucket")
          .contentType(MediaType.APPLICATION_JSON)
          .accept(MediaType.APPLICATION_JSON)
          .content(MAPPER.writeValueAsString(CreateVectorBucketRequest(vectorBucketName = "vector-bucket"))),
      ).andExpect(status().isOk)
      .andExpect(content().json("""{"vectorBucketArn":"arn:aws:s3vectors:us-east-1:123456789012:bucket/vector-bucket"}"""))
  }
}
