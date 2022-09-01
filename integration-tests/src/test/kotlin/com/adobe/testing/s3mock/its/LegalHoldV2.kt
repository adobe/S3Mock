/*
 *  Copyright 2017-2022 Adobe.
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
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File

class LegalHoldV2 : S3TestBase() {

  @Test
  fun testGetLegalHoldDefault() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3ClientV2!!.createBucket(CreateBucketRequest.builder().bucket(S3TestBase.BUCKET_NAME).build())
    s3ClientV2!!.putObject(
      PutObjectRequest.builder().bucket(S3TestBase.BUCKET_NAME).key(sourceKey).build(),
      RequestBody.fromFile(uploadFile)
    )
    val objectLegalHold = s3ClientV2!!.getObjectLegalHold(
      GetObjectLegalHoldRequest
        .builder()
        .bucket(BUCKET_NAME)
        .key(sourceKey)
        .build()
    )
    assertThat(objectLegalHold.legalHold().status()).isEqualTo(ObjectLockLegalHoldStatus.OFF)
  }

  @Test
  fun testPutAndGetLegalHold() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3ClientV2!!.createBucket(CreateBucketRequest.builder().bucket(S3TestBase.BUCKET_NAME).build())
    s3ClientV2!!.putObject(
      PutObjectRequest.builder().bucket(S3TestBase.BUCKET_NAME).key(sourceKey).build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2!!.putObjectLegalHold(PutObjectLegalHoldRequest
      .builder()
      .bucket(BUCKET_NAME)
      .key(sourceKey)
      .legalHold(ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build())
      .build())

    val objectLegalHold = s3ClientV2!!.getObjectLegalHold(
      GetObjectLegalHoldRequest
        .builder()
        .bucket(BUCKET_NAME)
        .key(sourceKey)
        .build()
    )
    assertThat(objectLegalHold.legalHold().status()).isEqualTo(ObjectLockLegalHoldStatus.ON)
  }

  companion object {
    const val BUCKET_NAME = "legal-hold-v2"
  }
}
