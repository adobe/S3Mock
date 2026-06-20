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
import software.amazon.awssdk.services.s3vectors.model.PutInputVector
import software.amazon.awssdk.services.s3vectors.model.VectorData

/**
 * Integration tests for S3 Vectors CRUD operations (PutVectors, GetVectors, ListVectors, DeleteVectors).
 */
internal class VectorIT : S3TestBase() {
  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `PutVectors and GetVectors round-trip with data and metadata`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, dimension = 3, distanceMetric = "cosine")

    vectorsClient.putVectors {
      it.vectorBucketName(bucketName)
      it.indexName(indexName)
      it.vectors(
        listOf(
          PutInputVector
            .builder()
            .key("v1")
            .data(VectorData.fromFloat32(listOf(1.0f, 0.0f, 0.0f)))
            .metadata(
              software.amazon.awssdk.core.document.Document
                .mapBuilder()
                .putString("genre", "drama")
                .build(),
            ).build(),
        ),
      )
    }

    val getResponse =
      vectorsClient.getVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.keys(listOf("v1"))
        it.returnData(true)
        it.returnMetadata(true)
      }

    assertThat(getResponse.vectors()).hasSize(1)
    assertThat(getResponse.vectors()[0].key()).isEqualTo("v1")
    assertThat(getResponse.vectors()[0].data().float32()).containsExactly(1.0f, 0.0f, 0.0f)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `PutVectors is upsert and re-putting replaces the vector`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, dimension = 2, distanceMetric = "euclidean")

    putVector(bucketName, indexName, "k1", listOf(1.0f, 0.0f))
    putVector(bucketName, indexName, "k1", listOf(0.0f, 1.0f))

    val response =
      vectorsClient.getVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.keys(listOf("k1"))
        it.returnData(true)
      }
    assertThat(response.vectors()[0].data().float32()).containsExactly(0.0f, 1.0f)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `ListVectors returns all vectors`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, dimension = 2, distanceMetric = "euclidean")

    putVector(bucketName, indexName, "a", listOf(1.0f, 0.0f))
    putVector(bucketName, indexName, "b", listOf(0.0f, 1.0f))

    val response =
      vectorsClient.listVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
      }

    assertThat(response.vectors().map { it.key() }).containsExactlyInAnyOrder("a", "b")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `ListVectors paginates correctly`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, dimension = 2, distanceMetric = "euclidean")
    (1..5).forEach { i -> putVector(bucketName, indexName, "v$i", listOf(i.toFloat(), 0.0f)) }

    val page1 =
      vectorsClient.listVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.maxResults(3)
      }
    assertThat(page1.vectors()).hasSize(3)
    assertThat(page1.nextToken()).isNotNull

    val page2 =
      vectorsClient.listVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
        it.maxResults(3)
        it.nextToken(page1.nextToken())
      }
    assertThat(page2.vectors()).hasSize(2)
    assertThat(page2.nextToken()).isNull()
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `DeleteVectors removes vectors`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName, dimension = 2, distanceMetric = "euclidean")
    putVector(bucketName, indexName, "del-key", listOf(1.0f, 0.0f))

    vectorsClient.deleteVectors {
      it.vectorBucketName(bucketName)
      it.indexName(indexName)
      it.keys(listOf("del-key"))
    }

    val response =
      vectorsClient.listVectors {
        it.vectorBucketName(bucketName)
        it.indexName(indexName)
      }
    assertThat(response.vectors().map { it.key() }).doesNotContain("del-key")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `PutVectors and GetVectors round-trip over HTTPS`(testInfo: TestInfo) {
    val bucketName = vectorBucketName(testInfo)
    vectorsClient.createVectorBucket { it.vectorBucketName(bucketName) }
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName("idx")
      it.dataType("float32")
      it.dimension(2)
      it.distanceMetric("cosine")
    }

    vectorsClient.putVectors {
      it.vectorBucketName(bucketName)
      it.indexName("idx")
      it.vectors(
        listOf(
          PutInputVector
            .builder()
            .key("h1")
            .data(VectorData.fromFloat32(listOf(1.0f, 0.0f)))
            .build(),
        ),
      )
    }

    val response =
      vectorsClient.getVectors {
        it.vectorBucketName(bucketName)
        it.indexName("idx")
        it.keys(listOf("h1"))
        it.returnData(true)
      }
    assertThat(response.vectors()).hasSize(1)
    assertThat(response.vectors()[0].data().float32()).containsExactly(1.0f, 0.0f)
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  private fun givenIndex(
    bucketName: String,
    dimension: Int,
    distanceMetric: String,
  ): String {
    val name = "test-index"
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName(name)
      it.dataType("float32")
      it.dimension(dimension)
      it.distanceMetric(distanceMetric)
    }
    return name
  }

  private fun putVector(
    bucketName: String,
    indexName: String,
    key: String,
    floats: List<Float>,
  ) {
    vectorsClient.putVectors {
      it.vectorBucketName(bucketName)
      it.indexName(indexName)
      it.vectors(
        listOf(
          PutInputVector
            .builder()
            .key(key)
            .data(VectorData.fromFloat32(floats))
            .build(),
        ),
      )
    }
  }
}
