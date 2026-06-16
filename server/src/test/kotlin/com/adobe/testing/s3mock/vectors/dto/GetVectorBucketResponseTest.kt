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

internal class GetVectorBucketResponseTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut =
      GetVectorBucketResponse(
        vectorBucket =
          VectorBucket(
            vectorBucketArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/my-vector-bucket",
            vectorBucketName = "my-vector-bucket",
            creationTime = 1.0,
            encryptionConfiguration =
              EncryptionConfiguration(
                sseType = "aws:kms",
                kmsKeyArn = "arn:aws:kms:us-east-1:123456789012:key/mrk-test",
              ),
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  @Test
  fun testSerializationWithoutEncryption(testInfo: TestInfo) {
    val iut =
      GetVectorBucketResponse(
        vectorBucket =
          VectorBucket(
            vectorBucketArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/plain-bucket",
            vectorBucketName = "plain-bucket",
            creationTime = 1.0,
            encryptionConfiguration = null,
          ),
      )

    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
