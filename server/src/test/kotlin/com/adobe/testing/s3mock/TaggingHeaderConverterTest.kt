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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.dto.Tag
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class TaggingHeaderConverterTest {

  private val iut = TaggingHeaderConverter()

  @Test
  fun `returns null for empty tags`() {
    val actual = iut.convert("")
    assertThat(actual).isNull()
  }

  @Test
  fun `converts single tag`() {
    val singleTag = tag(1)
    val actual = iut.convert(singleTag)
    assertThat(actual).isNotEmpty().hasSize(1)
    assertThat(requireNotNull(actual)).containsExactly(Tag(singleTag))
  }

  @Test
  fun `converts multiple tags`() {
    val tags = (0 until 5).map { tag(it) }
    val actual = iut.convert(tags.joinToString("&"))
    assertThat(actual)
      .isNotEmpty()
      .hasSize(5)
      .containsOnly(
        Tag(tag(0)),
        Tag(tag(1)),
        Tag(tag(2)),
        Tag(tag(3)),
        Tag(tag(4))
      )
  }

  private companion object {
    fun tag(i: Int): String = "tag$i=value$i"
  }
}
