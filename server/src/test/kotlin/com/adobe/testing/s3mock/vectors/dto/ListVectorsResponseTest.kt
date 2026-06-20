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
package com.adobe.testing.s3mock.vectors.dto

import com.adobe.testing.s3mock.DtoTestUtil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class ListVectorsResponseTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      ListVectorsResponse(
        vectors =
          listOf(
            ListOutputVector(key = "doc-1", data = null, metadata = null),
            ListOutputVector(key = "doc-2", data = null, metadata = null),
            ListOutputVector(key = "doc-3", data = null, metadata = null),
          ),
        nextToken = "token-page2",
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  @Test
  fun testSerializationWithData(testInfo: TestInfo) {
    val iut =
      ListVectorsResponse(
        vectors =
          listOf(
            ListOutputVector(
              key = "doc-1",
              data = VectorData(float32 = listOf(1.0, 0.0)),
              metadata = null,
            ),
          ),
        nextToken = null,
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
