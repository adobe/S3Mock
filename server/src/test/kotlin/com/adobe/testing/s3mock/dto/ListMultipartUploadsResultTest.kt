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
import java.util.Date

internal class ListMultipartUploadsResultTest {
  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListMultipartUploadsResult(
        "bucketName", "keyMarker", "/", "prefix/", "uploadIdMarker",
        2, false,
        "nextKeyMarker", "nextUploadIdMarker", createMultipartUploads(2),
        listOf(Prefix("prefix1/"), Prefix("prefix2/"))
      )
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }

  private fun createMultipartUploads(count: Int): List<MultipartUpload> {
    val multipartUploads = ArrayList<MultipartUpload>()
    for (i in 0 until count) {
      val multipartUpload =
        MultipartUpload(
          "key$i", "uploadId$i",
          Owner("displayName10$i", (10L + i).toString()),
          Owner("displayName100$i", (100L + i).toString()),
          StorageClass.STANDARD,
          Date(1514477008120L)
        )
      multipartUploads.add(multipartUpload)
    }
    return multipartUploads
  }
}
