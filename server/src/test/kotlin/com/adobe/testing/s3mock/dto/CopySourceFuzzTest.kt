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

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable

internal class CopySourceFuzzTest {
  @FuzzTest
  fun `fuzz copy source parsing`(data: FuzzedDataProvider) {
    val copySource = data.consumeRemainingAsString()
    val thrown = catchThrowable { CopySource.from(copySource) }
    assertThat(thrown).satisfiesAnyOf(
      { t -> assertThat(t).isNull() },
      { t -> assertThat(t).isInstanceOf(IllegalArgumentException::class.java) },
    )
  }
}
