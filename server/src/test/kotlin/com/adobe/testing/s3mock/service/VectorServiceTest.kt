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

import com.adobe.testing.s3mock.dto.PutInputVector
import com.adobe.testing.s3mock.dto.S3Metadata
import com.adobe.testing.s3mock.dto.VectorData
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
        PutInputVector(key = "zero", data = VectorData(listOf(0.0, 0.0, 0.0)), metadata = S3Metadata(emptyMap())),
        PutInputVector(key = "unit-x", data = VectorData(listOf(1.0, 0.0, 0.0)), metadata = S3Metadata(emptyMap())),
      ),
    )

    val response = iut.queryVectors("vectors", "idx", null, 2, VectorData(listOf(1.0, 0.0, 0.0)), false, true)

    assertThat(response.vectors).hasSize(2)
    assertThat(response.vectors!![0].key).isEqualTo("unit-x")
    assertThat(response.vectors[0].distance).isEqualTo(0.0)
    assertThat(response.vectors[1].key).isEqualTo("zero")
    assertThat(response.vectors[1].distance).isEqualTo(1.0)
  }

  @Test
  fun `list vectors applies default and maximum pagination limits`() {
    val iut = VectorService(VectorStore("us-east-1"))
    iut.createVectorBucket("vectors", null, emptyMap())
    iut.createIndex("vectors", null, "idx", "FLOAT32", 2, "EUCLIDEAN", null, null, emptyMap())

    val vectors =
      (0 until 1100).map { index ->
        PutInputVector(key = "k-$index", data = VectorData(listOf(index.toDouble(), 0.0)), metadata = null)
      }
    iut.putVectors("vectors", "idx", null, vectors)

    val defaultPage = iut.listVectors("vectors", "idx", null, null, null, false, false)
    assertThat(defaultPage.vectors).hasSize(100)
    assertThat(defaultPage.nextToken).isEqualTo("100")

    val cappedPage = iut.listVectors("vectors", "idx", null, 5000, null, false, false)
    assertThat(cappedPage.vectors).hasSize(1000)
    assertThat(cappedPage.nextToken).isEqualTo("1000")

    val invalidTokenPage = iut.listVectors("vectors", "idx", null, null, "invalid", false, false)
    assertThat(invalidTokenPage.vectors).hasSize(100)
    assertThat(invalidTokenPage.nextToken).isEqualTo("100")
  }
}
