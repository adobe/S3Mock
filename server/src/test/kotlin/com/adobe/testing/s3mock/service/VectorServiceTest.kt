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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.store.VectorRecord
import com.adobe.testing.s3mock.store.VectorStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class VectorServiceTest {
  @Test
  fun `query vectors ranks cosine distance and handles zero vectors`() {
    val iut = VectorService(VectorStore("us-east-1"))
    iut.createVectorBucket("vectors", null, emptyMap())
    iut.createIndex("vectors", null, "idx", "FLOAT32", 3, "COSINE", null, null, emptyMap())

    iut.putVectors(
      "vectors",
      "idx",
      null,
      listOf(
        VectorRecord("zero", listOf(0.0, 0.0, 0.0), emptyMap()),
        VectorRecord("unit-x", listOf(1.0, 0.0, 0.0), emptyMap()),
      ),
    )

    val response = iut.queryVectors("vectors", "idx", null, 2, listOf(1.0, 0.0, 0.0), false, true)
    @Suppress("UNCHECKED_CAST")
    val vectors = response["Vectors"] as List<Map<String, Any?>>

    assertThat(vectors).hasSize(2)
    assertThat(vectors[0]["Key"]).isEqualTo("unit-x")
    assertThat(vectors[0]["Distance"]).isEqualTo(0.0)
    assertThat(vectors[1]["Key"]).isEqualTo("zero")
    assertThat(vectors[1]["Distance"]).isEqualTo(1.0)
  }

  @Test
  fun `list vectors applies default and maximum pagination limits`() {
    val iut = VectorService(VectorStore("us-east-1"))
    iut.createVectorBucket("vectors", null, emptyMap())
    iut.createIndex("vectors", null, "idx", "FLOAT32", 2, "EUCLIDEAN", null, null, emptyMap())

    val vectors =
      (0 until 1100).map { index ->
        VectorRecord("k-$index", listOf(index.toDouble(), 0.0), emptyMap())
      }
    iut.putVectors("vectors", "idx", null, vectors)

    val defaultPage = iut.listVectors("vectors", "idx", null, null, null, false, false)
    @Suppress("UNCHECKED_CAST")
    val defaultVectors = defaultPage["Vectors"] as List<Map<String, Any?>>
    assertThat(defaultVectors).hasSize(100)
    assertThat(defaultPage["NextToken"]).isEqualTo("100")

    val cappedPage = iut.listVectors("vectors", "idx", null, 5000, null, false, false)
    @Suppress("UNCHECKED_CAST")
    val cappedVectors = cappedPage["Vectors"] as List<Map<String, Any?>>
    assertThat(cappedVectors).hasSize(1000)
    assertThat(cappedPage["NextToken"]).isEqualTo("1000")

    val invalidTokenPage = iut.listVectors("vectors", "idx", null, null, "invalid", false, false)
    @Suppress("UNCHECKED_CAST")
    val invalidTokenVectors = invalidTokenPage["Vectors"] as List<Map<String, Any?>>
    assertThat(invalidTokenVectors).hasSize(100)
    assertThat(invalidTokenPage["NextToken"]).isEqualTo("100")
  }
}
