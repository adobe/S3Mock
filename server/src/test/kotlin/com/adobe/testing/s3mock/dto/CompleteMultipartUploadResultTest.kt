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

import com.adobe.testing.s3mock.store.MultipartUploadInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException

internal class CompleteMultipartUploadResultTest {
  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val iut = CompleteMultipartUploadResult.from(
      "location",
      "bucket",
      "key",
      "etag",
      MultipartUploadInfo(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        listOf(Tag("key", "value")),
        "checksumSHA256",
        ChecksumType.COMPOSITE,
        ChecksumAlgorithm.SHA256
      ),
      "checksumSHA256",
      ChecksumType.COMPOSITE,
      ChecksumAlgorithm.SHA256,
      "versionId"
    )
    assertThat(iut).isNotNull()
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
