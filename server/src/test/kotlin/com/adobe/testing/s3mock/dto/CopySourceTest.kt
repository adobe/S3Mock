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
package com.adobe.testing.s3mock.dto

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * Verifies parsing behaviour from [CopySource].
 */
internal class CopySourceTest {
  @Test
  fun fromPrefixedCopySourceString() {
    val copySource = CopySource("/$VALID_COPY_SOURCE")

    assertThat(copySource.bucket).isEqualTo(BUCKET)
    assertThat(copySource.key).isEqualTo(KEY)
  }

  @Test
  fun fromCopySourceString() {
    val copySource = CopySource(VALID_COPY_SOURCE)

    assertThat(copySource.bucket).isEqualTo(BUCKET)
    assertThat(copySource.key).isEqualTo(KEY)
  }

  @Test
  fun invalidCopySource() {
    assertThatThrownBy {
      CopySource(UUID.randomUUID().toString())
    }
      .isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun nullCopySource() {
    assertThatThrownBy {
      CopySource(null)
    }
      .isInstanceOf(NullPointerException::class.java)
  }

  @Test
  @Disabled
  fun fromCopySourceWithVersion() {
    val copySource = CopySource(COPY_SOURCE_WITH_VERSION)

    assertThat(copySource.bucket).isEqualTo(BUCKET)
    assertThat(copySource.key).isEqualTo(KEY)
  }

  companion object {
    private val BUCKET = UUID.randomUUID().toString()
    private val KEY = UUID.randomUUID().toString()
    private val VALID_COPY_SOURCE = "$BUCKET/$KEY"
    private val COPY_SOURCE_WITH_VERSION = "$VALID_COPY_SOURCE?versionId=123"
  }
}
