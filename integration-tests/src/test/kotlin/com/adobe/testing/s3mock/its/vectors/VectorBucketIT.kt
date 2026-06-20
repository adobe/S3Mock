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
import software.amazon.awssdk.services.s3vectors.model.NotFoundException

/**
 * Integration tests for S3 Vectors bucket operations.
 */
internal class VectorBucketIT : S3TestBase() {
  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `creating and deleting a vector bucket is successful`(testInfo: TestInfo) {
    val bucketName = vectorBucketName(testInfo)

    val createResponse = vectorsClient.createVectorBucket { it.vectorBucketName(bucketName) }
    assertThat(createResponse.vectorBucketArn()).contains(bucketName)

    val getResponse = vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) }
    assertThat(getResponse.vectorBucket().vectorBucketName()).isEqualTo(bucketName)
    assertThat(getResponse.vectorBucket().creationTime()).isNotNull

    vectorsClient.deleteVectorBucket { it.vectorBucketName(bucketName) }

    assertThatThrownBy { vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) } }
      .isInstanceOf(NotFoundException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `creating the same bucket twice throws ConflictException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)

    assertThatThrownBy { vectorsClient.createVectorBucket { it.vectorBucketName(bucketName) } }
      .isInstanceOf(ConflictException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `listVectorBuckets returns created buckets`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)

    val listed = vectorsClient.listVectorBuckets { }.vectorBuckets().map { it.vectorBucketName() }
    assertThat(listed).contains(bucketName)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `listVectorBuckets filters by prefix`(testInfo: TestInfo) {
    val name = givenVectorBucket(testInfo)
    val prefix = name.take(5)

    val response = vectorsClient.listVectorBuckets { it.prefix(prefix) }
    assertThat(response.vectorBuckets()).isNotEmpty
    assertThat(response.vectorBuckets()).allMatch { it.vectorBucketName().startsWith(prefix) }
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `getting a non-existing bucket throws NotFoundException`() {
    assertThatThrownBy { vectorsClient.getVectorBucket { it.vectorBucketName("this-bucket-does-not-exist") } }
      .isInstanceOf(NotFoundException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `deleting a bucket with indexes throws ConflictException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    vectorsClient.createIndex {
      it.vectorBucketName(bucketName)
      it.indexName("some-index")
      it.dataType("float32")
      it.dimension(4)
      it.distanceMetric("cosine")
    }

    assertThatThrownBy { vectorsClient.deleteVectorBucket { it.vectorBucketName(bucketName) } }
      .isInstanceOf(ConflictException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `getVectorBucket by ARN succeeds`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val arn = vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) }.vectorBucket().vectorBucketArn()

    val response = vectorsClient.getVectorBucket { it.vectorBucketArn(arn) }
    assertThat(response.vectorBucket().vectorBucketName()).isEqualTo(bucketName)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `creating and getting a vector bucket over HTTPS is successful`(testInfo: TestInfo) {
    val bucketName = vectorBucketName(testInfo)

    val createResponse = vectorsClient.createVectorBucket { it.vectorBucketName(bucketName) }
    assertThat(createResponse.vectorBucketArn()).contains(bucketName)

    val getResponse = vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) }
    assertThat(getResponse.vectorBucket().vectorBucketName()).isEqualTo(bucketName)

    vectorsClient.deleteVectorBucket { it.vectorBucketName(bucketName) }
  }
}
