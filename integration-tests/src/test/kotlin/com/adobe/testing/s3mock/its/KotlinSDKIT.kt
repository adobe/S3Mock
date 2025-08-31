/*
 *  Copyright 2017-2025 Adobe.
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

import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import aws.sdk.kotlin.services.s3.model.DeleteBucketRequest
import aws.sdk.kotlin.services.s3.model.HeadBucketRequest
import aws.sdk.kotlin.services.s3.model.S3Exception
import aws.sdk.kotlin.services.s3.waiters.waitUntilBucketExists
import aws.sdk.kotlin.services.s3.waiters.waitUntilBucketNotExists
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class KotlinSDKIT : S3TestBase() {
  private val s3Client = createS3ClientKotlin()

  @Test
  @S3VerifiedFailure(year = 2025,
    reason = "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to.")
  fun createAndDeleteBucket(testInfo: TestInfo) : Unit = runBlocking {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(CreateBucketRequest { bucket = bucketName })

    s3Client.waitUntilBucketExists(HeadBucketRequest { bucket = bucketName })

    //does not throw exception if bucket exists.
    s3Client.headBucket(HeadBucketRequest { bucket = bucketName })

    s3Client.deleteBucket(DeleteBucketRequest { bucket = bucketName })
    s3Client.waitUntilBucketNotExists(HeadBucketRequest { bucket = bucketName })

    //throws exception if bucket does not exist.
    assertThatThrownBy {
      runBlocking {
        s3Client.headBucket(HeadBucketRequest { bucket = bucketName })
      }
    }.isInstanceOf(S3Exception::class.java)
  }
}
