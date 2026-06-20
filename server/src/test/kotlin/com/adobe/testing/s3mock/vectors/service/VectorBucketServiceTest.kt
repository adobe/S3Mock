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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.nio.file.Path

internal class VectorBucketServiceTest {
  private val vectorBucketStore: VectorBucketStore = mock()
  private val iut = VectorBucketService(vectorBucketStore, "us-east-1")

  @Test
  fun `createVectorBucket creates bucket and returns ARN`() {
    whenever(vectorBucketStore.doesBucketExist("my-bucket")).thenReturn(false)
    whenever(
      vectorBucketStore.createVectorBucket("my-bucket", null, null, emptyMap()),
    ).thenReturn(bucketMetadata("my-bucket"))

    val response = iut.createVectorBucket("my-bucket", null, emptyMap())

    assertThat(response.vectorBucketArn).contains("my-bucket").startsWith("arn:aws:s3vectors:")
  }

  @Test
  fun `createVectorBucket throws ConflictException when bucket already exists`() {
    whenever(vectorBucketStore.doesBucketExist("existing")).thenReturn(true)

    assertThatThrownBy { iut.createVectorBucket("existing", null, emptyMap()) }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_ALREADY_EXISTS)
  }

  @Test
  fun `createVectorBucket throws ValidationException for invalid name`() {
    assertThatThrownBy { iut.createVectorBucket("A", null, emptyMap()) }
      .isEqualTo(S3VectorsException.INVALID_BUCKET_NAME)
  }

  @Test
  fun `getVectorBucket returns bucket for existing name`() {
    whenever(vectorBucketStore.doesBucketExist("found-bucket")).thenReturn(true)
    whenever(vectorBucketStore.getBucketMetadata("found-bucket")).thenReturn(bucketMetadata("found-bucket"))

    val response = iut.getVectorBucket("found-bucket")

    assertThat(response.vectorBucket.vectorBucketName).isEqualTo("found-bucket")
  }

  @Test
  fun `getVectorBucket resolves ARN to name`() {
    val arn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/arn-bucket"
    whenever(vectorBucketStore.doesBucketExist("arn-bucket")).thenReturn(true)
    whenever(vectorBucketStore.getBucketMetadata("arn-bucket")).thenReturn(bucketMetadata("arn-bucket"))

    val response = iut.getVectorBucket(arn)

    assertThat(response.vectorBucket.vectorBucketName).isEqualTo("arn-bucket")
  }

  @Test
  fun `getVectorBucket throws NotFoundException for non-existing bucket`() {
    whenever(vectorBucketStore.doesBucketExist("missing")).thenReturn(false)

    assertThatThrownBy { iut.getVectorBucket("missing") }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_NOT_FOUND)
  }

  @Test
  fun `listVectorBuckets returns all buckets sorted by name`() {
    whenever(vectorBucketStore.listBuckets()).thenReturn(
      listOf(bucketMetadata("bucket-b"), bucketMetadata("bucket-a")),
    )

    val response = iut.listVectorBuckets(null, null, null)

    assertThat(response.vectorBuckets.map { it.vectorBucketName }).containsExactly("bucket-a", "bucket-b")
    assertThat(response.nextToken).isNull()
  }

  @Test
  fun `listVectorBuckets paginates with nextToken`() {
    whenever(vectorBucketStore.listBuckets()).thenReturn(
      listOf(bucketMetadata("b1"), bucketMetadata("b2"), bucketMetadata("b3")),
    )

    val first = iut.listVectorBuckets(maxResults = 2, nextToken = null, prefix = null)
    assertThat(first.vectorBuckets).hasSize(2)
    assertThat(first.nextToken).isNotNull

    val second = iut.listVectorBuckets(maxResults = 2, nextToken = first.nextToken, prefix = null)
    assertThat(second.vectorBuckets).hasSize(1)
    assertThat(second.nextToken).isNull()
  }

  @Test
  fun `listVectorBuckets filters by prefix`() {
    whenever(vectorBucketStore.listBuckets()).thenReturn(
      listOf(bucketMetadata("prod-a"), bucketMetadata("prod-b"), bucketMetadata("test-a")),
    )

    val response = iut.listVectorBuckets(null, null, "prod-")

    assertThat(response.vectorBuckets.map { it.vectorBucketName }).containsExactly("prod-a", "prod-b")
  }

  @Test
  fun `deleteVectorBucket removes bucket when no indexes exist`() {
    whenever(vectorBucketStore.doesBucketExist("empty-bucket")).thenReturn(true)
    whenever(vectorBucketStore.getBucketMetadata("empty-bucket")).thenReturn(bucketMetadata("empty-bucket"))
    whenever(vectorBucketStore.hasIndexes("empty-bucket")).thenReturn(false)

    iut.deleteVectorBucket("empty-bucket")
  }

  @Test
  fun `deleteVectorBucket throws ConflictException when bucket has indexes`() {
    whenever(vectorBucketStore.doesBucketExist("full-bucket")).thenReturn(true)
    whenever(vectorBucketStore.getBucketMetadata("full-bucket")).thenReturn(bucketMetadata("full-bucket"))
    whenever(vectorBucketStore.hasIndexes("full-bucket")).thenReturn(true)

    assertThatThrownBy { iut.deleteVectorBucket("full-bucket") }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_NOT_EMPTY)
  }

  @Test
  fun `deleteVectorBucket throws NotFoundException for non-existing bucket`() {
    whenever(vectorBucketStore.doesBucketExist("ghost")).thenReturn(false)

    assertThatThrownBy { iut.deleteVectorBucket("ghost") }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_NOT_FOUND)
  }

  private fun bucketMetadata(name: String) =
    VectorBucketMetadata(
      name = name,
      creationTime = System.currentTimeMillis(),
      sseType = null,
      kmsKeyArn = null,
      tags = emptyMap(),
      path = Path.of("/tmp"),
    )
}
