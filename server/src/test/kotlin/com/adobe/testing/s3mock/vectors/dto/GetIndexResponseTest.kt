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
package com.adobe.testing.s3mock.vectors.dto

import com.adobe.testing.s3mock.DtoTestUtil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class GetIndexResponseTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      GetIndexResponse(
        index =
          VectorIndex(
            indexArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket/index/my-index",
            indexName = "my-index",
            vectorBucketName = "my-vector-bucket",
            dataType = "float32",
            dimension = 1536,
            distanceMetric = "cosine",
            creationTime = 1.0,
            encryptionConfiguration =
              EncryptionConfiguration(
                sseType = "aws:kms",
                kmsKeyArn = "arn:aws:kms:us-east-1:123456789012:key/mrk-test",
              ),
            metadataConfiguration =
              MetadataConfiguration(
                nonFilterableMetadataKeys = listOf("embedding-model"),
              ),
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  @Test
  fun testSerializationMinimal(testInfo: TestInfo) {
    val iut =
      GetIndexResponse(
        index =
          VectorIndex(
            indexArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket/index/my-index",
            indexName = "my-index",
            vectorBucketName = "my-vector-bucket",
            dataType = "float32",
            dimension = 4,
            distanceMetric = "euclidean",
            creationTime = 1.0,
            encryptionConfiguration = null,
            metadataConfiguration = null,
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
