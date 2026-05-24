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
package com.adobe.testing.s3mock.dto

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

internal class S3DataTypesTest {
  private val jsonMapper: ObjectMapper =
    JsonMapper
      .builder()
      .addModule(KotlinModule.Builder().build())
      .findAndAddModules()
      .build()

  @Test
  fun `s3 tags supports flat object serialization and deserialization`() {
    val parsed = jsonMapper.readValue("""{"suite":"integration"}""", S3Tags::class.java)

    assertThat(parsed.values).containsEntry("suite", "integration")
    assertThat(jsonMapper.writeValueAsString(parsed)).isEqualTo("""{"suite":"integration"}""")
  }

  @Test
  fun `s3 metadata supports flat object serialization and deserialization`() {
    val parsed = jsonMapper.readValue("""{"label":"A"}""", S3Metadata::class.java)

    assertThat(parsed.values).containsEntry("label", "A")
    assertThat(jsonMapper.writeValueAsString(parsed)).isEqualTo("""{"label":"A"}""")
  }
}
