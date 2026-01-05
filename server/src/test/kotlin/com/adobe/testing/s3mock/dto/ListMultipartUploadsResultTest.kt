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

internal class ListMultipartUploadsResultTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListMultipartUploadsResult(
        "bucketName",
        listOf(Prefix("prefix1/"), Prefix("prefix2/")),
        "/",
        "encodingType",
        false,
        "keyMarker",
        2,
        "nextKeyMarker",
        "nextUploadIdMarker",
        "prefix/",
        createMultipartUploads(2),
        "uploadIdMarker",
      )
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }

  private fun createMultipartUploads(count: Int): List<MultipartUpload> {
    return (0 until count).map {
      MultipartUpload(
        ChecksumAlgorithm.SHA256,
        ChecksumType.COMPOSITE,
        Date(1514477008120L),
        Initiator("displayName100$it", (100L + it).toString()),
        "key$it",
        Owner((10L + it).toString()),
        StorageClass.STANDARD,
        "uploadId$it",
      )
    }
  }
}
