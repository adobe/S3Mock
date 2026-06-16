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
import com.adobe.testing.s3mock.its.S3VerifiedFailure
import com.adobe.testing.s3mock.its.S3VerifiedSuccess
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3vectors.model.NotFoundException

internal class VectorPolicyIT : S3TestBase() {
  @Test
  @S3VerifiedFailure(
    year = 2026,
    reason = "Principal not available on test AWS account.",
  )
  fun `putting and getting a policy returns the stored policy content`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val policy =
      """
      {"Version": "2012-10-17","Statement": [{"Effect": "Allow","Principal": {
      "AWS": "arn:aws:iam::111122223333:root"},"Action": "s3vectors:*",
      "Resource": "arn:aws:s3vectors:aws-region:111122223333:bucket/amzn-s3-demo-vector-bucket"}]}
      """.trimIndent()

    vectorsClient.putVectorBucketPolicy { it.vectorBucketName(bucketName).policy(policy) }

    val retrieved = vectorsClient.getVectorBucketPolicy { it.vectorBucketName(bucketName) }.policy()
    assertThat(retrieved).isEqualTo(policy)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `deleting a policy and then getting it throws NotFoundException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val policy =
      """
      {"Version": "2012-10-17","Statement": [{"Effect": "Allow","Principal": {
      "AWS": "arn:aws:iam::111122223333:root"},"Action": "s3vectors:*",
      "Resource": "arn:aws:s3vectors:aws-region:111122223333:bucket/amzn-s3-demo-vector-bucket"}]}
      """.trimIndent()

    vectorsClient.putVectorBucketPolicy { it.vectorBucketName(bucketName).policy(policy) }
    vectorsClient.deleteVectorBucketPolicy { it.vectorBucketName(bucketName) }

    assertThatThrownBy { vectorsClient.getVectorBucketPolicy { it.vectorBucketName(bucketName) } }
      .isInstanceOf(NotFoundException::class.java)
  }

  @Test
  @S3VerifiedFailure(
    year = 2026,
    reason = "Principal not available on test AWS account.",
  )
  fun `getting a policy that was never set throws NotFoundException`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)

    assertThatThrownBy { vectorsClient.getVectorBucketPolicy { it.vectorBucketName(bucketName) } }
      .isInstanceOf(NotFoundException::class.java)
  }
}
