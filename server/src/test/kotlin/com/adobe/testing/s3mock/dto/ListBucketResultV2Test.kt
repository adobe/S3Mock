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

internal class ListBucketResultV2Test {
  @Test
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
        "2",
        1000,
        "bucketName",
        "nextContinuationToken",
        "prefix/",
        "startAfter"
      )
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }

  private fun createBucketContents(count: Int): List<S3Object> {
    return (0 until count).map {
      S3Object(
        ChecksumAlgorithm.SHA256,
        ChecksumType.FULL_OBJECT,
        "\"fba9dede5f27731c9771645a39863328\"",
        "key$it",
        "2009-10-12T17:50:30.000Z",
        Owner((10L + it).toString()),
        null,
        "434234",
        StorageClass.STANDARD
      )
    }
  }
}
