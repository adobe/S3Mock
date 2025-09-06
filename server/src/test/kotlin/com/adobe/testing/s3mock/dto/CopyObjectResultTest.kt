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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class CopyObjectResultTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut = CopyObjectResult("2017-12-28T16:03:28.120Z", "99f2fdceebf20fb2e891810adfb0eb71")
    assertThat(iut).isNotNull()
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
