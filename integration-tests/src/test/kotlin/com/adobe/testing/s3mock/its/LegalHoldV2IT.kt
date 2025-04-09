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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.File

internal class LegalHoldV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetLegalHoldNoBucketLockConfiguration(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)

    assertThatThrownBy {
      s3ClientV2.getObjectLegalHold {
        it.bucket(bucketName)
        it.key(sourceKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Bucket is missing Object Lock Configuration")
      .hasMessageContaining("Service: S3, Status Code: 400")
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetLegalHoldNoObjectLockConfiguration(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket {
      it.bucket(bucketName)
      it.objectLockEnabledForBucket(true)
    }
    s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      },
      RequestBody.fromFile(uploadFile)
    )

    assertThatThrownBy {
      s3ClientV2.getObjectLegalHold {
        it.bucket(bucketName)
        it.key(sourceKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("The specified object does not have a ObjectLock configuration")
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPutAndGetLegalHold(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket {
      it.bucket(bucketName)
      it.objectLockEnabledForBucket(true)
    }
    s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.putObjectLegalHold {
      it.bucket(bucketName)
      it.key(sourceKey)
      it.legalHold {
        it.status(ObjectLockLegalHoldStatus.ON)
      }
    }

    s3ClientV2.getObjectLegalHold {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.also {
      assertThat(it.legalHold().status()).isEqualTo(ObjectLockLegalHoldStatus.ON)
    }

    s3ClientV2.putObjectLegalHold {
      it.bucket(bucketName)
      it.key(sourceKey)
      it.legalHold {
        it.status(ObjectLockLegalHoldStatus.OFF)
      }
    }

    s3ClientV2.getObjectLegalHold {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.also {
      assertThat(it.legalHold().status()).isEqualTo(ObjectLockLegalHoldStatus.OFF)
    }
  }
}
