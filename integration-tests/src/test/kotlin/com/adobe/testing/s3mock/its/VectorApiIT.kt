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
import software.amazon.awssdk.services.s3vectors.model.DataType
import software.amazon.awssdk.services.s3vectors.model.DistanceMetric
import software.amazon.awssdk.services.s3vectors.model.PutInputVector
import software.amazon.awssdk.services.s3vectors.model.VectorData

internal class VectorApiIT : S3TestBase() {
  @Test
  fun `supports s3 vectors api lifecycle`(testInfo: TestInfo) {
    createS3VectorsClient().use { s3VectorsClient ->
      val vectorBucketName = "${bucketName(testInfo)}-vectors"

      val createVectorBucketResponse =
        s3VectorsClient.createVectorBucket {
          it.vectorBucketName(vectorBucketName)
          it.tags(mapOf("suite" to "integration"))
        }
      assertThat(createVectorBucketResponse.vectorBucketArn()).isNotBlank

      val getVectorBucketResponse =
        s3VectorsClient.getVectorBucket {
          it.vectorBucketName(vectorBucketName)
        }
      assertThat(getVectorBucketResponse.vectorBucket().vectorBucketName()).isEqualTo(vectorBucketName)

      val listVectorBucketsResponse =
        s3VectorsClient.listVectorBuckets {
          it.prefix(vectorBucketName.take(6))
          it.maxResults(10)
        }
      assertThat(listVectorBucketsResponse.vectorBuckets()).anyMatch { it.vectorBucketName() == vectorBucketName }

      val createIndexResponse =
        s3VectorsClient.createIndex {
          it.vectorBucketName(vectorBucketName)
          it.indexName("products")
          it.dataType(DataType.FLOAT32)
          it.dimension(3)
          it.distanceMetric(DistanceMetric.COSINE)
          it.tags(mapOf("domain" to "catalog"))
        }
      val indexArn = createIndexResponse.indexArn()
      assertThat(indexArn).isNotBlank

      val getIndexResponse =
        s3VectorsClient.getIndex {
          it.indexArn(indexArn)
        }
      assertThat(getIndexResponse.index().indexName()).isEqualTo("products")
      assertThat(getIndexResponse.index().dimension()).isEqualTo(3)

      val listIndexesResponse =
        s3VectorsClient.listIndexes {
          it.vectorBucketName(vectorBucketName)
          it.maxResults(10)
        }
      assertThat(listIndexesResponse.indexes()).anyMatch { it.indexArn() == indexArn }

      s3VectorsClient.putVectors {
        it.indexArn(indexArn)
        it.vectors(
          listOf(
            PutInputVector
              .builder()
              .key("vector-a")
              .data(VectorData.builder().float32(listOf(1.0f, 0.0f, 0.0f)).build())
              .metadata(mapOf("label" to "A"))
              .build(),
            PutInputVector
              .builder()
              .key("vector-b")
              .data(VectorData.builder().float32(listOf(0.0f, 1.0f, 0.0f)).build())
              .metadata(mapOf("label" to "B"))
              .build(),
          ),
        )
      }

      val getVectorsResponse =
        s3VectorsClient.getVectors {
          it.indexArn(indexArn)
          it.keys(listOf("vector-a", "vector-b"))
          it.returnData(true)
          it.returnMetadata(true)
        }
      assertThat(getVectorsResponse.vectors()).hasSize(2)

      val listVectorsPage1 =
        s3VectorsClient.listVectors {
          it.indexArn(indexArn)
          it.maxResults(1)
          it.returnData(true)
          it.returnMetadata(true)
        }
      assertThat(listVectorsPage1.vectors()).hasSize(1)
      assertThat(listVectorsPage1.nextToken()).isNotBlank

      val listVectorsPage2 =
        s3VectorsClient.listVectors {
          it.indexArn(indexArn)
          it.maxResults(1)
          it.nextToken(listVectorsPage1.nextToken())
        }
      assertThat(listVectorsPage2.vectors()).hasSize(1)

      val queryVectorsResponse =
        s3VectorsClient.queryVectors {
          it.indexArn(indexArn)
          it.topK(1)
          it.queryVector(VectorData.builder().float32(listOf(1.0f, 0.0f, 0.0f)).build())
          it.returnDistance(true)
          it.returnMetadata(true)
        }
      assertThat(queryVectorsResponse.vectors()).hasSize(1)
      assertThat(queryVectorsResponse.vectors().first().key()).isEqualTo("vector-a")
      assertThat(queryVectorsResponse.distanceMetric()).isEqualTo(DistanceMetric.COSINE)

      val policy = """{"Version":"2012-10-17","Statement":[]}"""
      s3VectorsClient.putVectorBucketPolicy {
        it.vectorBucketName(vectorBucketName)
        it.policy(policy)
      }
      val getPolicyResponse =
        s3VectorsClient.getVectorBucketPolicy {
          it.vectorBucketName(vectorBucketName)
        }
      assertThat(getPolicyResponse.policy()).isEqualTo(policy)

      s3VectorsClient.deleteVectorBucketPolicy {
        it.vectorBucketName(vectorBucketName)
      }
      val policyAfterDelete =
        s3VectorsClient.getVectorBucketPolicy {
          it.vectorBucketName(vectorBucketName)
        }
      assertThat(policyAfterDelete.policy()).isEmpty()

      s3VectorsClient.tagResource {
        it.resourceArn(indexArn)
        it.tags(mapOf("owner" to "test"))
      }
      val listTagsResponse =
        s3VectorsClient.listTagsForResource {
          it.resourceArn(indexArn)
        }
      assertThat(listTagsResponse.tags()).containsEntry("owner", "test")

      s3VectorsClient.untagResource {
        it.resourceArn(indexArn)
        it.tagKeys(listOf("owner"))
      }
      val listTagsAfterUntag =
        s3VectorsClient.listTagsForResource {
          it.resourceArn(indexArn)
        }
      assertThat(listTagsAfterUntag.tags()).doesNotContainKey("owner")

      s3VectorsClient.deleteVectors {
        it.indexArn(indexArn)
        it.keys(listOf("vector-a", "vector-b"))
      }
      val listAfterDeleteVectors =
        s3VectorsClient.listVectors {
          it.indexArn(indexArn)
        }
      assertThat(listAfterDeleteVectors.vectors()).isEmpty()

      s3VectorsClient.deleteIndex {
        it.indexArn(indexArn)
      }
      val listIndexesAfterDelete =
        s3VectorsClient.listIndexes {
          it.vectorBucketName(vectorBucketName)
        }
      assertThat(listIndexesAfterDelete.indexes()).isEmpty()

      s3VectorsClient.deleteVectorBucket {
        it.vectorBucketName(vectorBucketName)
      }
      val listBucketsAfterDelete =
        s3VectorsClient.listVectorBuckets {
          it.prefix(vectorBucketName)
        }
      assertThat(listBucketsAfterDelete.vectorBuckets()).noneMatch { it.vectorBucketName() == vectorBucketName }
    }
  }
}
