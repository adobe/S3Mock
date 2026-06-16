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
import com.adobe.testing.s3mock.vectors.store.VectorBucketMetadata
import com.adobe.testing.s3mock.vectors.store.VectorBucketStore
import com.adobe.testing.s3mock.vectors.store.VectorIndexMetadata
import com.adobe.testing.s3mock.vectors.store.VectorIndexStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.nio.file.Path

internal class VectorIndexServiceTest {
  private val vectorBucketStore: VectorBucketStore = mock()
  private val vectorBucketService = VectorBucketService(vectorBucketStore, "us-east-1")
  private val vectorIndexStore: VectorIndexStore = mock()
  private val iut = VectorIndexService(vectorBucketService, vectorIndexStore, "us-east-1")

  @Test
  fun `createIndex returns ARN when bucket exists and index is new`() {
    givenBucket("my-bucket")
    whenever(vectorIndexStore.doesIndexExist("my-bucket", "my-index")).thenReturn(false)
    whenever(
      vectorIndexStore.createIndex("my-bucket", "my-index", "float32", 128, "cosine", null, null, emptyList(), emptyMap()),
    ).thenReturn(indexMetadata("my-bucket", "my-index", 128))

    val response = iut.createIndex("my-bucket", "my-index", "float32", 128, "cosine", null, null, emptyMap())

    assertThat(response.indexArn).contains("my-index").startsWith("arn:aws:s3vectors:")
  }

  @Test
  fun `createIndex throws ConflictException when index already exists`() {
    givenBucket("b")
    whenever(vectorIndexStore.doesIndexExist("b", "existing")).thenReturn(true)

    assertThatThrownBy { iut.createIndex("b", "existing", "float32", 4, "cosine", null, null, emptyMap()) }
      .isEqualTo(S3VectorsException.INDEX_ALREADY_EXISTS)
  }

  @Test
  fun `createIndex throws ValidationException for dimension out of range`() {
    givenBucket("b")
    assertThatThrownBy { iut.createIndex("b", "bad-dim", "float32", 5000, "cosine", null, null, emptyMap()) }
      .isEqualTo(S3VectorsException.INVALID_DIMENSION)
  }

  @Test
  fun `getIndex returns index when it exists`() {
    givenBucket("b")
    whenever(vectorIndexStore.doesIndexExist("b", "i")).thenReturn(true)
    whenever(vectorIndexStore.getIndexMetadata("b", "i")).thenReturn(indexMetadata("b", "i", 64))

    val response = iut.getIndex("b", "i")

    assertThat(response.index.indexName).isEqualTo("i")
    assertThat(response.index.dimension).isEqualTo(64)
  }

  @Test
  fun `getIndex resolves ARN`() {
    val arn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/b/index/i"
    whenever(vectorIndexStore.doesIndexExist("b", "i")).thenReturn(true)
    whenever(vectorIndexStore.getIndexMetadata("b", "i")).thenReturn(indexMetadata("b", "i", 64))

    val response = iut.getIndex(null, arn)

    assertThat(response.index.indexName).isEqualTo("i")
  }

  @Test
  fun `getIndex throws NotFoundException when index does not exist`() {
    givenBucket("b")
    whenever(vectorIndexStore.doesIndexExist("b", "ghost")).thenReturn(false)

    assertThatThrownBy { iut.getIndex("b", "ghost") }
      .isEqualTo(S3VectorsException.INDEX_NOT_FOUND)
  }

  @Test
  fun `listIndexes returns indexes sorted by name`() {
    givenBucket("b")
    whenever(vectorIndexStore.listIndexes("b")).thenReturn(
      listOf(indexMetadata("b", "idx-b", 4), indexMetadata("b", "idx-a", 4)),
    )

    val response = iut.listIndexes("b", null, null, null)

    assertThat(response.indexes.map { it.indexName }).containsExactly("idx-a", "idx-b")
  }

  @Test
  fun `listIndexes filters by prefix`() {
    givenBucket("b")
    whenever(vectorIndexStore.listIndexes("b")).thenReturn(
      listOf(indexMetadata("b", "prod-idx", 4), indexMetadata("b", "test-idx", 4)),
    )

    val response = iut.listIndexes("b", "prod-", null, null)

    assertThat(response.indexes.map { it.indexName }).containsExactly("prod-idx")
  }

  @Test
  fun `deleteIndex removes index when it exists`() {
    givenBucket("b")
    whenever(vectorIndexStore.doesIndexExist("b", "del-idx")).thenReturn(true)
    whenever(vectorIndexStore.getIndexMetadata("b", "del-idx")).thenReturn(indexMetadata("b", "del-idx", 4))

    iut.deleteIndex("b", "del-idx")
  }

  @Test
  fun `deleteIndex throws NotFoundException for non-existing index`() {
    givenBucket("b")
    whenever(vectorIndexStore.doesIndexExist("b", "ghost")).thenReturn(false)

    assertThatThrownBy { iut.deleteIndex("b", "ghost") }
      .isEqualTo(S3VectorsException.INDEX_NOT_FOUND)
  }

  private fun givenBucket(name: String) {
    whenever(vectorBucketStore.doesBucketExist(name)).thenReturn(true)
    whenever(vectorBucketStore.getBucketMetadata(name)).thenReturn(
      VectorBucketMetadata(name, System.currentTimeMillis(), null, null, emptyMap(), Path.of("/tmp")),
    )
  }

  private fun indexMetadata(
    bucketName: String,
    indexName: String,
    dimension: Int,
  ) = VectorIndexMetadata(
    name = indexName,
    vectorBucketName = bucketName,
    dataType = "float32",
    dimension = dimension,
    distanceMetric = "cosine",
    creationTime = System.currentTimeMillis(),
    sseType = null,
    kmsKeyArn = null,
    nonFilterableMetadataKeys = emptyList(),
    tags = emptyMap(),
    path = Path.of("/tmp"),
  )
}
