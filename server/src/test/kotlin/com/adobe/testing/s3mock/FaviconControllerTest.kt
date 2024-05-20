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

import com.adobe.testing.s3mock.store.KmsKeyStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType

@MockBean(classes = [KmsKeyStore::class, ObjectController::class, BucketController::class, MultipartController::class])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class FaviconControllerTest : BaseControllerTest() {
  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  fun testFavicon() {
    val headers = HttpHeaders()
    headers.accept = listOf(MediaType.APPLICATION_JSON)
    val response = restTemplate.exchange(
      "/favicon.ico",
      HttpMethod.GET,
      HttpEntity<Any>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }
}
