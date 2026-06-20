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
package com.adobe.testing.s3mock.vectors.service

import com.adobe.testing.s3mock.vectors.S3VectorsException
import com.adobe.testing.s3mock.vectors.dto.VectorData
import com.adobe.testing.s3mock.vectors.store.VectorIndexMetadata
import com.adobe.testing.s3mock.vectors.store.VectorStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import java.nio.file.Path
import kotlin.math.sqrt

internal class VectorQueryServiceTest {
  private val vectorIndexService: VectorIndexService = mock()
  private val vectorStore: VectorStore = mock()
  private val iut = VectorQueryService(vectorIndexService, vectorStore)
  private val mapper =
    JsonMapper
      .builder()
      .addModule(KotlinModule.Builder().build())
      .findAndAddModules()
      .build()

  // ── Distance functions ─────────────────────────────────────────────────

  @Test
  fun `euclidean distance between identical vectors is zero`() {
    val v = floatArrayOf(1f, 2f, 3f)
    givenIndex(distanceMetric = "euclidean", dimension = 3)
    givenVectors(listOf(v to null))

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(1.0, 2.0, 3.0)),
        topK = 1,
        filter = null,
        returnDistance = true,
        returnMetadata = false,
      )

    assertThat(result.vectors).hasSize(1)
    assertThat(result.vectors[0].distance).isCloseTo(0.0, Offset.offset(1e-6))
  }

  @Test
  fun `euclidean distance for orthogonal unit vectors is sqrt(2)`() {
    givenIndex(distanceMetric = "euclidean", dimension = 2)
    givenVectors(listOf(floatArrayOf(1f, 0f) to null))

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(0.0, 1.0)),
        topK = 1,
        filter = null,
        returnDistance = true,
        returnMetadata = false,
      )

    assertThat(result.vectors[0].distance).isCloseTo(sqrt(2.0), Offset.offset(1e-6))
  }

  @Test
  fun `cosine distance between identical vectors is zero`() {
    val v = floatArrayOf(1f, 0f, 0f)
    givenIndex(distanceMetric = "cosine", dimension = 3)
    givenVectors(listOf(v to null))

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(1.0, 0.0, 0.0)),
        topK = 1,
        filter = null,
        returnDistance = true,
        returnMetadata = false,
      )

    assertThat(result.vectors[0].distance).isCloseTo(0.0, Offset.offset(1e-6))
  }

  @Test
  fun `cosine distance for orthogonal vectors is 1`() {
    givenIndex(distanceMetric = "cosine", dimension = 2)
    givenVectors(listOf(floatArrayOf(1f, 0f) to null))

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(0.0, 1.0)),
        topK = 1,
        filter = null,
        returnDistance = true,
        returnMetadata = false,
      )

    assertThat(result.vectors[0].distance).isCloseTo(1.0, Offset.offset(1e-6))
  }

  @Test
  fun `topK limits results to k nearest`() {
    givenIndex(distanceMetric = "euclidean", dimension = 1)
    givenVectors(
      listOf(
        floatArrayOf(1f) to null,
        floatArrayOf(3f) to null,
        floatArrayOf(5f) to null,
      ),
    )

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(0.0)),
        topK = 2,
        filter = null,
        returnDistance = true,
        returnMetadata = false,
      )

    assertThat(result.vectors).hasSize(2)
    assertThat(result.vectors[0].distance).isLessThan(result.vectors[1].distance!!)
  }

  @Test
  fun `topK greater than 100 throws ValidationException`() {
    givenIndex(distanceMetric = "euclidean", dimension = 1)

    assertThatThrownBy {
      iut.queryVectors(null, "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i", VectorData(listOf(1.0)), 101, null, false, false)
    }.isEqualTo(S3VectorsException.INVALID_TOP_K)
  }

  // ── Filter DSL ────────────────────────────────────────────────────────

  @Test
  fun `filter $eq matches string value`() {
    val metadata = mapper.readTree("""{"genre":"drama"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}eq":"drama"}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter implicit $eq (bare value) matches`() {
    val metadata = mapper.readTree("""{"genre":"drama"}""")
    val filter = mapper.readTree("""{"genre":"drama"}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $eq does not match different value`() {
    val metadata = mapper.readTree("""{"genre":"comedy"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}eq":"drama"}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isFalse()
  }

  @Test
  fun `filter $ne matches when value differs`() {
    val metadata = mapper.readTree("""{"genre":"comedy"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}ne":"drama"}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $gt matches when field is greater`() {
    val metadata = mapper.readTree("""{"year":2023}""")
    val filter = mapper.readTree("""{"year":{"${'$'}gt":2020}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $gt does not match when field is equal`() {
    val metadata = mapper.readTree("""{"year":2020}""")
    val filter = mapper.readTree("""{"year":{"${'$'}gt":2020}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isFalse()
  }

  @Test
  fun `filter $gte matches when field is equal`() {
    val metadata = mapper.readTree("""{"year":2020}""")
    val filter = mapper.readTree("""{"year":{"${'$'}gte":2020}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $lt matches when field is less`() {
    val metadata = mapper.readTree("""{"rating":3.5}""")
    val filter = mapper.readTree("""{"rating":{"${'$'}lt":4.0}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $lte matches when field is equal`() {
    val metadata = mapper.readTree("""{"rating":4.0}""")
    val filter = mapper.readTree("""{"rating":{"${'$'}lte":4.0}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $in matches when value is in set`() {
    val metadata = mapper.readTree("""{"genre":"drama"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}in":["drama","comedy"]}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $nin excludes value in set`() {
    val metadata = mapper.readTree("""{"genre":"horror"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}nin":["drama","comedy"]}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $exists true matches present field`() {
    val metadata = mapper.readTree("""{"genre":"drama"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}exists":true}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $exists false matches missing field`() {
    val metadata = mapper.readTree("""{"other":"value"}""")
    val filter = mapper.readTree("""{"genre":{"${'$'}exists":false}}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $and requires all conditions`() {
    val metadata = mapper.readTree("""{"genre":"drama","year":2023}""")
    val filter = mapper.readTree("""{"${'$'}and":[{"genre":"drama"},{"year":{"${'$'}gt":2020}}]}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $and fails when one condition is false`() {
    val metadata = mapper.readTree("""{"genre":"drama","year":2019}""")
    val filter = mapper.readTree("""{"${'$'}and":[{"genre":"drama"},{"year":{"${'$'}gt":2020}}]}""")

    assertThat(iut.matchesFilter(metadata, filter)).isFalse()
  }

  @Test
  fun `filter $or matches when at least one condition is true`() {
    val metadata = mapper.readTree("""{"genre":"horror"}""")
    val filter = mapper.readTree("""{"${'$'}or":[{"genre":"drama"},{"genre":"horror"}]}""")

    assertThat(iut.matchesFilter(metadata, filter)).isTrue()
  }

  @Test
  fun `filter $or fails when no condition matches`() {
    val metadata = mapper.readTree("""{"genre":"documentary"}""")
    val filter = mapper.readTree("""{"${'$'}or":[{"genre":"drama"},{"genre":"horror"}]}""")

    assertThat(iut.matchesFilter(metadata, filter)).isFalse()
  }

  @Test
  fun `filter with non-filterable key throws ValidationException`() {
    givenIndex(distanceMetric = "euclidean", dimension = 1, nonFilterableKeys = listOf("internal"))
    givenVectors(emptyList())
    val filter = mapper.readTree("""{"internal":"value"}""")

    assertThatThrownBy {
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(0.0)),
        topK = 1,
        filter = filter,
        returnDistance = false,
        returnMetadata = false,
      )
    }.isEqualTo(S3VectorsException.INVALID_FILTER_NON_FILTERABLE_KEY)
  }

  @Test
  fun `filter with null metadata does not match non-null condition`() {
    val filter = mapper.readTree("""{"genre":"drama"}""")

    assertThat(iut.matchesFilter(null, filter)).isFalse()
  }

  @Test
  fun `returns distanceMetric from index`() {
    givenIndex(distanceMetric = "cosine", dimension = 1)
    givenVectors(emptyList())

    val result =
      iut.queryVectors(
        bucketNameOrArn = null,
        indexNameOrArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i",
        queryVector = VectorData(listOf(1.0)),
        topK = 1,
        filter = null,
        returnDistance = false,
        returnMetadata = false,
      )

    assertThat(result.distanceMetric).isEqualTo("cosine")
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private fun givenIndex(
    distanceMetric: String,
    dimension: Int,
    nonFilterableKeys: List<String> = emptyList(),
  ) {
    val indexMeta =
      VectorIndexMetadata(
        name = "i",
        vectorBucketName = "b",
        dataType = "float32",
        dimension = dimension,
        distanceMetric = distanceMetric,
        creationTime = System.currentTimeMillis(),
        sseType = null,
        kmsKeyArn = null,
        nonFilterableMetadataKeys = nonFilterableKeys,
        tags = emptyMap(),
        path = Path.of("/tmp"),
      )
    whenever(vectorIndexService.resolveIndexId(null, "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i"))
      .thenReturn("b" to "i")
    whenever(vectorIndexService.requireIndex("b", "i")).thenReturn(indexMeta)
  }

  private fun givenVectors(entries: List<Pair<FloatArray, tools.jackson.databind.JsonNode?>>) {
    val stored =
      entries.mapIndexed { idx, (floats, meta) ->
        VectorStore.StoredVector("key-$idx", floats, meta)
      }
    whenever(vectorStore.readAllForQuery("b", "i")).thenReturn(stored)
  }
}
