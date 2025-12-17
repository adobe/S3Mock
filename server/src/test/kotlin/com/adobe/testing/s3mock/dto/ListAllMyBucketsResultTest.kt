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
import java.nio.file.Paths

internal class ListAllMyBucketsResultTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListAllMyBucketsResult(
        Owner(10L.toString()),
        createBuckets(),
        "some-prefix",
        "50d8e003-0451-48fd-9c49-8208b151649c"
      )
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }

  private fun createBuckets(count: Int = 2): Buckets {
    val buckets = (0 until count).map {
      Bucket(
        "us-east-1",
        "creationDate",
        "name-$it",
        Paths.get("/tmp/foo")
      )
    }
    return Buckets(ArrayList(buckets))
  }
}
