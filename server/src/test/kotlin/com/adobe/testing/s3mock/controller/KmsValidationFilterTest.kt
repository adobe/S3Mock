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

import com.adobe.testing.s3mock.store.KmsKeyStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@MockitoBean(types = [KmsKeyStore::class])
@WebMvcTest(
  controllers = [KmsValidationFilterTest.DummyPassController::class],
  properties = ["com.adobe.testing.s3mock.store.region=us-east-1"],
  useDefaultFilters = false
)
@Import(KmsValidationFilterTest.Config::class)
internal class KmsValidationFilterTest {

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Autowired
  private lateinit var kmsKeyStore: KmsKeyStore


  @Test
  fun `denies request with invalid kms key id`() {
    val badKey = "bad-key-id"
    whenever(kmsKeyStore.validateKeyId(badKey)).thenReturn(false)

    mockMvc.perform(
      get("/internal-test/pass")
        .param("dummy", "true")
        .header("X-Dummy", "1")
        .header("x-amz-server-side-encryption", "aws:kms")
        .header("x-amz-server-side-encryption-aws-kms-key-id", badKey)
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().contentType(MediaType.APPLICATION_XML))
      .andExpect(content().string(org.hamcrest.Matchers.containsString("Invalid keyId '$badKey'")))
  }

  @Test
  fun `allows request with valid kms key id`() {
    val goodKey = "good-key-id"
    whenever(kmsKeyStore.validateKeyId(goodKey)).thenReturn(true)

    mockMvc.perform(
      get("/internal-test/pass")
        .param("dummy", "true")
        .header("X-Dummy", "1")
        .header("x-amz-server-side-encryption", "aws:kms")
        .header("x-amz-server-side-encryption-aws-kms-key-id", goodKey)
    )
      .andExpect { assertThat(it.response.status).isNotEqualTo(400) }
  }

  @Test
  fun `allows request when kms headers are missing`() {
    mockMvc.perform(
      get("/internal-test/pass")
        .param("dummy", "true")
        .header("X-Dummy", "1")
    ).andExpect { assertThat(it.response.status).isNotEqualTo(400) }
  }

  @TestConfiguration
  internal class Config {
    @Bean
    fun kmsValidationFilter(
      kmsKeyStore: KmsKeyStore
    ) = KmsValidationFilter(kmsKeyStore, ControllerConfiguration().messageConverter())
  }

  @RestController
  @RequestMapping("/internal-test", params = ["dummy=true"])
  internal class DummyPassController {
    @GetMapping("/pass", headers = ["X-Dummy=1"])
    fun pass(): ResponseEntity<String> = ResponseEntity.ok("ok")
  }
}
