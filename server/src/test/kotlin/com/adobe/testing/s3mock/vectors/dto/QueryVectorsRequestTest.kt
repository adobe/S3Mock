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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class QueryVectorsRequestTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(QueryVectorsRequest::class.java, testInfo)

    assertThat(iut.vectorBucketName).isEqualTo("my-vector-bucket")
    assertThat(iut.indexName).isEqualTo("my-index")
    assertThat(iut.queryVector?.float32).containsExactly(0.2, 0.5, 0.1)
    assertThat(iut.topK).isEqualTo(10)
    assertThat(iut.returnDistance).isTrue()
    assertThat(iut.returnMetadata).isTrue()
    // filter: {"genre": {"$eq": "drama"}} — navigate into the $eq operator node
    assertThat(
      iut.filter
        ?.get("genre")
        ?.get("\$eq")
        ?.asText(),
    ).isEqualTo("drama")
  }

  @Test
  fun testDeserializationWithComplexFilter(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(QueryVectorsRequest::class.java, testInfo)

    assertThat(iut.queryVector?.float32).containsExactly(1.0, 0.0)
    assertThat(iut.topK).isEqualTo(5)
    assertThat(iut.returnDistance).isFalse()
    // $and filter with two conditions
    val andNode = iut.filter?.get("\$and")
    assertThat(andNode).isNotNull()
    assertThat(andNode!!.isArray).isTrue()
    assertThat(andNode.size()).isEqualTo(2)
  }
}
