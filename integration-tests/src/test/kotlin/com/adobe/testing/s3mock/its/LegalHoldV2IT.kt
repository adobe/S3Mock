/*
 *  Copyright 2017-2024 Adobe.
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

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.File

internal class LegalHoldV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetLegalHoldNoBucketLockConfiguration(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)

    assertThatThrownBy {
      s3ClientV2.getObjectLegalHold(
        GetObjectLegalHoldRequest
          .builder()
          .bucket(bucketName)
          .key(sourceKey)
          .build()
      )
    }.isInstanceOf(S3Exception::class.java)
     .hasMessageContaining("Bucket is missing Object Lock Configuration")
     .hasMessageContaining("Service: S3, Status Code: 400")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetLegalHoldNoObjectLockConfiguration(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName)
      .objectLockEnabledForBucket(true).build())
    s3ClientV2.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(sourceKey).build(),
      RequestBody.fromFile(uploadFile)
    )

    assertThatThrownBy {
      s3ClientV2.getObjectLegalHold(
        GetObjectLegalHoldRequest
          .builder()
          .bucket(bucketName)
          .key(sourceKey)
          .build()
      )
    }.isInstanceOf(S3Exception::class.java)
     .hasMessageContaining("The specified object does not have a ObjectLock configuration")
     .hasMessageContaining("Service: S3, Status Code: 404")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutAndGetLegalHold(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest
      .builder()
      .bucket(bucketName)
      .objectLockEnabledForBucket(true)
      .build()
    )
    s3ClientV2.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(sourceKey).build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.putObjectLegalHold(PutObjectLegalHoldRequest
      .builder()
      .bucket(bucketName)
      .key(sourceKey)
      .legalHold(ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build())
      .build()
    )

    s3ClientV2.getObjectLegalHold(
      GetObjectLegalHoldRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    ).also {
      assertThat(it.legalHold().status()).isEqualTo(ObjectLockLegalHoldStatus.ON)
    }
  }
}
