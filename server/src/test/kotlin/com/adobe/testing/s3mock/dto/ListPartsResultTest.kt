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

import com.adobe.testing.s3mock.DtoTestUtil.serializeAndAssertXML
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.util.Date

internal class ListPartsResultTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListPartsResult(
          "bucketName",
          ChecksumAlgorithm.CRC32,
          ChecksumType.COMPOSITE,
          Initiator("id", "displayName"),
          false,
          "fileName",
          1000,
          100,
          Owner("displayName"),
          createParts(),
          5,
          StorageClass.STANDARD,
          "uploadId",
      )
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }

  private fun createParts(count: Int = 2): List<Part> {
    return (1..count).map { Part(it, "etag$it", Date(1514477008120L), 10L + it) }
  }
}
