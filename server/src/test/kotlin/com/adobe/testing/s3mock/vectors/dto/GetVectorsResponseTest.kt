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

internal class GetVectorsResponseTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val factory = JsonNodeFactory.instance
    val metadata =
      factory.objectNode().apply {
        put("genre", "drama")
        put("year", 2024)
      }

    val iut =
      GetVectorsResponse(
        vectors =
          listOf(
            GetOutputVector(
              key = "doc-1",
              data = VectorData(float32 = listOf(1.0, 0.0, 0.0)),
              metadata = metadata,
            ),
            GetOutputVector(
              key = "doc-2",
              data = VectorData(float32 = listOf(0.0, 1.0, 0.0)),
              metadata = null,
            ),
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
