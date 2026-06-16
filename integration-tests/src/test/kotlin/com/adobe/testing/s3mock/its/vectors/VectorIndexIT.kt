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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3vectors.model.ConflictException
import software.amazon.awssdk.services.s3vectors.model.DistanceMetric
import software.amazon.awssdk.services.s3vectors.model.NotFoundException

internal class VectorIndexIT : S3TestBase() {
  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `createIndex returns ARN with index name and metadata`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = "test-index"

    val arn =
      vectorsClient
        .createIndex {
          it.vectorBucketName(bucketName)
          it.indexName(indexName)
          it.dataType("float32")
          it.dimension(4)
          it.distanceMetric("cosine")
        }.indexArn()

    assertThat(arn).contains(indexName)

    val index = vectorsClient.getIndex { it.vectorBucketName(bucketName).indexName(indexName) }.index()
    assertThat(index.indexName()).isEqualTo(indexName)
    assertThat(index.indexArn()).isEqualTo(arn)
    assertThat(index.dimension()).isEqualTo(4)
    assertThat(index.distanceMetric()).isEqualTo(DistanceMetric.COSINE)
    assertThat(index.creationTime()).isNotNull()
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `getIndex by name succeeds`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName)

    val index = vectorsClient.getIndex { it.vectorBucketName(bucketName).indexName(indexName) }.index()
    assertThat(index.indexName()).isEqualTo(indexName)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `getIndex by ARN succeeds`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val arn =
      vectorsClient
        .createIndex {
          it.vectorBucketName(bucketName)
          it.indexName("arn-index")
          it.dataType("float32")
          it.dimension(4)
          it.distanceMetric("cosine")
        }.indexArn()

    val index = vectorsClient.getIndex { it.indexArn(arn) }.index()
    assertThat(index.indexName()).isEqualTo("arn-index")
    assertThat(index.indexArn()).isEqualTo(arn)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `listIndexes returns all created indexes`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    givenIndex(bucketName, "index-a")
    givenIndex(bucketName, "index-b")

    val names = vectorsClient.listIndexes { it.vectorBucketName(bucketName) }.indexes().map { it.indexName() }
    assertThat(names).contains("index-a", "index-b")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `listIndexes filters by prefix`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    givenIndex(bucketName, "prefix-one")
    givenIndex(bucketName, "prefix-two")
    givenIndex(bucketName, "other-index")

    val response = vectorsClient.listIndexes { it.vectorBucketName(bucketName).prefix("prefix-") }
    assertThat(response.indexes()).isNotEmpty
    assertThat(response.indexes()).allMatch { it.indexName().startsWith("prefix-") }
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `deleteIndex removes the index and getting it throws NotFoundException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexName = givenIndex(bucketName)

    vectorsClient.deleteIndex { it.vectorBucketName(bucketName).indexName(indexName) }

    assertThatThrownBy { vectorsClient.getIndex { it.vectorBucketName(bucketName).indexName(indexName) } }
      .isInstanceOf(NotFoundException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `creating the same index twice throws ConflictException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    givenIndex(bucketName)

    assertThatThrownBy {
      vectorsClient.createIndex {
        it.vectorBucketName(bucketName)
        it.indexName("test-index")
        it.dataType("float32")
        it.dimension(4)
        it.distanceMetric("cosine")
      }
    }.isInstanceOf(ConflictException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `getting a non-existing index throws NotFoundException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)

    assertThatThrownBy { vectorsClient.getIndex { it.vectorBucketName(bucketName).indexName("no-such-index") } }
      .isInstanceOf(NotFoundException::class.java)
  }

  private fun givenIndex(
    bucketName: String,
    indexName: String = "test-index",
  ): String {
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName(indexName)
      it.dataType("float32")
      it.dimension(4)
      it.distanceMetric("cosine")
    }
    return indexName
  }
}
