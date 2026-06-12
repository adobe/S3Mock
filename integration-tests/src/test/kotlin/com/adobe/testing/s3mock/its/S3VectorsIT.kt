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
package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3vectors.model.CreateIndexRequest
import software.amazon.awssdk.services.s3vectors.model.CreateVectorBucketRequest
import software.amazon.awssdk.services.s3vectors.model.DeleteIndexRequest
import software.amazon.awssdk.services.s3vectors.model.DeleteVectorBucketPolicyRequest
import software.amazon.awssdk.services.s3vectors.model.DeleteVectorBucketRequest
import software.amazon.awssdk.services.s3vectors.model.DeleteVectorsRequest
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest
import software.amazon.awssdk.services.s3vectors.model.ListIndexesRequest
import software.amazon.awssdk.services.s3vectors.model.ListTagsForResourceRequest
import software.amazon.awssdk.services.s3vectors.model.ListVectorBucketsRequest
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest
import software.amazon.awssdk.services.s3vectors.model.PutInputVector
import software.amazon.awssdk.services.s3vectors.model.PutVectorBucketPolicyRequest
import software.amazon.awssdk.services.s3vectors.model.PutVectorsRequest
import software.amazon.awssdk.services.s3vectors.model.QueryVectorsRequest
import software.amazon.awssdk.services.s3vectors.model.TagResourceRequest
import software.amazon.awssdk.services.s3vectors.model.UntagResourceRequest
import software.amazon.awssdk.services.s3vectors.model.VectorData

internal class S3VectorsIT : S3TestBase() {
  @Test
  fun `vector bucket lifecycle works`(testInfo: TestInfo) {
    val s3VectorsClient = createS3VectorsClient()
    val vectorBucketName = "bucket-${randomSuffix(testInfo)}"

    val createResponse =
      s3VectorsClient.createVectorBucket(
        CreateVectorBucketRequest.builder().vectorBucketName(vectorBucketName).build(),
      )

    assertThat(createResponse.vectorBucketArn()).contains("/bucket/$vectorBucketName")

    val listResponse =
      s3VectorsClient.listVectorBuckets(
        ListVectorBucketsRequest.builder().prefix(vectorBucketName).build(),
      )

    assertThat(listResponse.vectorBuckets()).hasSize(1)
    assertThat(listResponse.vectorBuckets()[0].vectorBucketName()).isEqualTo(vectorBucketName)

    s3VectorsClient.deleteVectorBucket(
      DeleteVectorBucketRequest.builder().vectorBucketName(vectorBucketName).build(),
    )

    val listAfterDelete =
      s3VectorsClient.listVectorBuckets(
        ListVectorBucketsRequest.builder().prefix(vectorBucketName).build(),
      )

    assertThat(listAfterDelete.vectorBuckets()).isEmpty()
  }

  @Test
  fun `index vectors query policy and tags work`(testInfo: TestInfo) {
    val s3VectorsClient = createS3VectorsClient()
    val vectorBucketName = "bucket-${randomSuffix(testInfo)}"
    val indexName = "index-${randomSuffix(testInfo)}"

    val bucketArn =
      s3VectorsClient
        .createVectorBucket(
          CreateVectorBucketRequest.builder().vectorBucketName(vectorBucketName).build(),
        ).vectorBucketArn()

    val indexArn =
      s3VectorsClient
        .createIndex(
          CreateIndexRequest
            .builder()
            .vectorBucketName(vectorBucketName)
            .indexName(indexName)
            .dataType("float32")
            .dimension(3)
            .distanceMetric("euclidean")
            .build(),
        ).indexArn()

    val indexes =
      s3VectorsClient.listIndexes(
        ListIndexesRequest.builder().vectorBucketName(vectorBucketName).build(),
      )

    assertThat(indexes.indexes()).hasSize(1)
    assertThat(indexes.indexes()[0].indexArn()).isEqualTo(indexArn)

    s3VectorsClient.putVectors(
      PutVectorsRequest
        .builder()
        .vectorBucketName(vectorBucketName)
        .indexName(indexName)
        .vectors(
          PutInputVector
            .builder()
            .key("k1")
            .data(VectorData.builder().float32(0.0f, 0.0f, 1.0f).build())
            .build(),
          PutInputVector
            .builder()
            .key("k2")
            .data(VectorData.builder().float32(0.0f, 1.0f, 0.0f).build())
            .build(),
        ).build(),
    )

    val listedVectors =
      s3VectorsClient.listVectors(
        ListVectorsRequest
          .builder()
          .vectorBucketName(vectorBucketName)
          .indexName(indexName)
          .returnData(true)
          .build(),
      )

    assertThat(listedVectors.vectors()).hasSize(2)

    val getVectors =
      s3VectorsClient.getVectors(
        GetVectorsRequest
          .builder()
          .vectorBucketName(vectorBucketName)
          .indexName(indexName)
          .keys("k1")
          .returnData(true)
          .build(),
      )

    assertThat(getVectors.vectors()).hasSize(1)
    assertThat(getVectors.vectors()[0].key()).isEqualTo("k1")

    val query =
      s3VectorsClient.queryVectors(
        QueryVectorsRequest
          .builder()
          .vectorBucketName(vectorBucketName)
          .indexName(indexName)
          .queryVector(VectorData.builder().float32(0.0f, 0.0f, 1.0f).build())
          .topK(1)
          .returnDistance(true)
          .build(),
      )

    assertThat(query.vectors()).hasSize(1)
    assertThat(query.vectors()[0].key()).isEqualTo("k1")

    s3VectorsClient.putVectorBucketPolicy(
      PutVectorBucketPolicyRequest
        .builder()
        .vectorBucketName(vectorBucketName)
        .policy("{\"Version\":\"2012-10-17\",\"Statement\":[]}")
        .build(),
    )

    assertThat(
      s3VectorsClient
        .getVectorBucketPolicy {
          it.vectorBucketName(vectorBucketName)
        }.policy(),
    ).contains("Statement")

    s3VectorsClient.tagResource(
      TagResourceRequest
        .builder()
        .resourceArn(bucketArn)
        .tags(mapOf("env" to "it"))
        .build(),
    )

    val tags =
      s3VectorsClient.listTagsForResource(
        ListTagsForResourceRequest.builder().resourceArn(bucketArn).build(),
      )

    assertThat(tags.tags()).containsEntry("env", "it")

    s3VectorsClient.untagResource(
      UntagResourceRequest
        .builder()
        .resourceArn(bucketArn)
        .tagKeys("env")
        .build(),
    )

    val untagged =
      s3VectorsClient.listTagsForResource(
        ListTagsForResourceRequest.builder().resourceArn(bucketArn).build(),
      )

    assertThat(untagged.tags()).doesNotContainKey("env")

    s3VectorsClient.deleteVectors(
      DeleteVectorsRequest
        .builder()
        .vectorBucketName(vectorBucketName)
        .indexName(indexName)
        .keys("k1", "k2")
        .build(),
    )

    s3VectorsClient.deleteIndex(
      DeleteIndexRequest
        .builder()
        .vectorBucketName(vectorBucketName)
        .indexName(indexName)
        .build(),
    )

    s3VectorsClient.deleteVectorBucketPolicy(
      DeleteVectorBucketPolicyRequest.builder().vectorBucketName(vectorBucketName).build(),
    )

    s3VectorsClient.deleteVectorBucket(
      DeleteVectorBucketRequest.builder().vectorBucketName(vectorBucketName).build(),
    )
  }

  private fun randomSuffix(testInfo: TestInfo): String =
    testInfo.testMethod
      .orElseThrow()
      .name
      .take(10)
      .lowercase() + randomName.take(8)
}
