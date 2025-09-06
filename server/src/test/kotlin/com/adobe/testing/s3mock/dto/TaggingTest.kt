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
package com.adobe.testing.s3mock.dto

import com.adobe.testing.s3mock.dto.DtoTestUtil.deserialize
import com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class TaggingTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut = Tagging(TagSet(listOf(createTag(0), createTag(1))))
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }

  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = deserialize(Tagging::class.java, testInfo)
    assertThat(iut.tagSet.tags).hasSize(2)

    iut.tagSet.tags[0].also {
      assertThat(it.key).isEqualTo("key0")
      assertThat(it.value).isEqualTo("val0")
    }

    iut.tagSet.tags[1].also {
      assertThat(it.key).isEqualTo("key1")
      assertThat(it.value).isEqualTo("val1")
    }
  }

  companion object {
    private fun createTag(counter: Int): Tag = Tag("key$counter", "val$counter")
  }
}
