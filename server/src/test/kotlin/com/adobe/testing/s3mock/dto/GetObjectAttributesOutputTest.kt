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

import com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException

internal class GetObjectAttributesOutputTest {
  @Test
  @Throws(IOException::class)
  fun testSerialization_object(testInfo: TestInfo) {
    val iut = GetObjectAttributesOutput(
      checksum(),
      "etag",
      null,
      1L,
      StorageClass.STANDARD
    )
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }

  @Test
  @Throws(IOException::class)
  fun testSerialization_multiPart(testInfo: TestInfo) {
    val part = ObjectPart(
        "checksumCRC32",
        "checksumCRC32C",
        "checksumCRC64NVME",
        "checksumSHA1",
        "checksumSHA256",
        1,
        1L
    )
    val getObjectAttributesParts = GetObjectAttributesParts(
      false,
      1000,
      0,
      0,
      listOf(part),
      0
    )
    val iut = GetObjectAttributesOutput(
      checksum(),
      "etag",
      listOf(getObjectAttributesParts),
      1L,
      StorageClass.STANDARD
    )
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }

  fun checksum(): Checksum = Checksum(
      "checksumCRC32",
      "checksumCRC32C",
      "checksumCRC64NVME",
      "checksumSHA1",
      "checksumSHA256",
      ChecksumType.FULL_OBJECT,
  )
}
