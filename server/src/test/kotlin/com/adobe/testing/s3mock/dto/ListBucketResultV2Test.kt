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

internal class ListBucketResultV2Test {
  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListBucketResultV2(
        listOf(
          Prefix("prefix1/"),
          Prefix("prefix2/")
        ),
        createBucketContents(2),
        "continuationToken",
        "delimiter",
        "url",
        false,
        1000,
        "bucketName",
        "nextContinuationToken",
        "prefix/",
        "2",
        "startAfter"
      )
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }

  private fun createBucketContents(count: Int): List<S3Object> {
    val s3ObjectList = ArrayList<S3Object>()
    for (i in 0 until count) {
      S3Object(
        ChecksumAlgorithm.SHA256,
        ChecksumType.FULL_OBJECT,
        "\"fba9dede5f27731c9771645a39863328\"",
        "key$i",
        "2009-10-12T17:50:30.000Z",
        Owner("displayName", (10L + i).toString()),
        null,
        "434234",
        StorageClass.STANDARD
      ).also {
        s3ObjectList.add(it)
      }
    }
    return s3ObjectList
  }
}
