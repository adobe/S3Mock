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
package com.adobe.testing.s3mock.its.vectors

import com.adobe.testing.s3mock.its.S3TestBase
import com.adobe.testing.s3mock.its.S3VerifiedSuccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.document.Document
import software.amazon.awssdk.services.s3vectors.model.DistanceMetric
import software.amazon.awssdk.services.s3vectors.model.PutInputVector
import software.amazon.awssdk.services.s3vectors.model.VectorData

/**
 * Integration tests for QueryVectors — similarity search with filters.
 */
internal class VectorQueryIT : S3TestBase() {
  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors returns nearest neighbor with euclidean distance`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, 3, "euclidean")

    putVectors(
      bucketName,
      indexName,
      listOf(
        Triple("close", listOf(1.0f, 0.0f, 0.0f), null),
        Triple("far", listOf(0.0f, 0.0f, 10.0f), null),
      ),
    )

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.queryVector(VectorData.fromFloat32(listOf(1.0f, 0.0f, 0.0f)))
        it.topK(1)
        it.returnDistance(true)
      }

    assertThat(response.vectors()).hasSize(1)
    assertThat(response.vectors()[0].key()).isEqualTo("close")
    assertThat(response.distanceMetric()).isEqualTo(DistanceMetric.EUCLIDEAN)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors returns nearest neighbor with cosine distance`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, 2, "cosine")

    putVectors(
      bucketName,
      indexName,
      listOf(
        Triple("aligned", listOf(1.0f, 0.0f), null),
        Triple("orthogonal", listOf(0.0f, 1.0f), null),
      ),
    )

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.queryVector(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
        it.topK(1)
        it.returnDistance(true)
      }

    assertThat(response.vectors()[0].key()).isEqualTo("aligned")
    assertThat(response.vectors()[0].distance()).isLessThan(0.01f)
    assertThat(response.distanceMetric()).isEqualTo(DistanceMetric.COSINE)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors respects topK limit`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, 1, "euclidean")
    (1..5).forEach { i -> putVectors(bucketName, indexName, listOf(Triple("v$i", listOf(i.toFloat()), null))) }

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.queryVector(VectorData.fromFloat32(listOf(0.0f)))
        it.topK(3)
      }

    assertThat(response.vectors()).hasSize(3)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors with metadata filter returns only matching vectors`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, 2, "cosine")

    putVectors(
      bucketName,
      indexName,
      listOf(
        Triple("drama-1", listOf(1.0f, 0.0f), Document.mapBuilder().putString("genre", "drama").build()),
        Triple("comedy-1", listOf(0.9f, 0.1f), Document.mapBuilder().putString("genre", "comedy").build()),
        Triple("drama-2", listOf(0.8f, 0.2f), Document.mapBuilder().putString("genre", "drama").build()),
      ),
    )

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.queryVector(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
        it.topK(10)
        it.filter(Document.mapBuilder().putString("genre", "drama").build())
        it.returnMetadata(true)
      }

    assertThat(response.vectors()).allMatch { it.key().startsWith("drama") }
    assertThat(response.vectors().map { it.key() }).doesNotContain("comedy-1")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors distance is not returned when returnDistance is false`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, 2, "euclidean")
    putVectors(bucketName, indexName, listOf(Triple("v1", listOf(1.0f, 0.0f), null)))

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.queryVector(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
        it.topK(1)
        it.returnDistance(false)
      }

    assertThat(response.vectors()[0].distance() as Float?).isNull()
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `QueryVectors returns nearest neighbor over HTTPS`(testInfo: TestInfo) {
    val bucketName = vectorBucketName(testInfo)
    vectorsClient.createVectorBucket { it.vectorBucketName(bucketName) }
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName("idx")
      it.dataType("float32")
      it.dimension(2)
      it.distanceMetric("euclidean")
    }
    vectorsClient.putVectors {
      it.vectorBucketName(bucketName)
      it.indexName("idx")
      it.vectors(
        listOf(
          PutInputVector
            .builder()
            .key("near")
            .data(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
            .build(),
          PutInputVector
            .builder()
            .key("far")
            .data(VectorData.fromFloat32(listOf(0.0f, 10.0f)))
            .build(),
        ),
      )
    }

    val response =
      vectorsClient.queryVectors {
        it.vectorBucketName(bucketName)
        it.indexName("idx")
        it.queryVector(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
        it.topK(1)
      }

    assertThat(response.vectors()).hasSize(1)
    assertThat(response.vectors()[0].key()).isEqualTo("near")
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  private fun givenIndex(
    bucketName: String,
    dimension: Int,
    distanceMetric: String,
  ): String {
    val name = "query-index"
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName(name)
      it.dataType("float32")
      it.dimension(dimension)
      it.distanceMetric(distanceMetric)
    }
    return name
  }

  private fun putVectors(
    bucketName: String,
    indexName: String,
    vectors: List<Triple<String, List<Float>, Document?>>,
  ) {
    vectorsClient.putVectors {
      it.vectorBucketName(bucketName)
      it.indexName(indexName)
      it.vectors(
        vectors.map { (key, floats, metadata) ->
          PutInputVector
            .builder()
            .key(key)
            .data(VectorData.fromFloat32(floats))
            .apply { if (metadata != null) metadata(metadata) }
            .build()
        },
      )
    }
  }
}
