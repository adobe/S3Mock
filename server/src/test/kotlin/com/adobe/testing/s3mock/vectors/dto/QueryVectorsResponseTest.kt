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
import tools.jackson.databind.node.JsonNodeFactory

internal class QueryVectorsResponseTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val factory = JsonNodeFactory.instance
    val metadata =
      factory.objectNode().apply {
        put("genre", "drama")
      }

    val iut =
      QueryVectorsResponse(
        distanceMetric = "cosine",
        vectors =
          listOf(
            QueryOutputVector(key = "doc-1", distance = 0.05, metadata = metadata),
            QueryOutputVector(key = "doc-2", distance = 0.42, metadata = null),
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  @Test
  fun testSerializationWithoutDistance(testInfo: TestInfo) {
    val iut =
      QueryVectorsResponse(
        distanceMetric = "euclidean",
        vectors =
          listOf(
            QueryOutputVector(key = "doc-1", distance = null, metadata = null),
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
