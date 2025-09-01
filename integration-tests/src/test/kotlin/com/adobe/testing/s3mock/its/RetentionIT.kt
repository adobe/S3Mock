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
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ObjectLockRetention
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode
import software.amazon.awssdk.services.s3.model.S3Exception
import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.MILLIS

internal class RetentionIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetRetentionNoBucketLockConfiguration(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)

    assertThatThrownBy {
      s3Client.getObjectRetention {
        it.bucket(bucketName)
        it.key(sourceKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Bucket is missing Object Lock Configuration")
      .hasMessageContaining("Service: S3, Status Code: 400")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetRetentionNoObjectLockConfiguration(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3Client.createBucket {
      it.bucket(bucketName)
      it.objectLockEnabledForBucket(true)
    }
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      },
      RequestBody.fromFile(UPLOAD_FILE)
    )

    assertThatThrownBy {
      s3Client.getObjectRetention {
        it.bucket(bucketName)
        it.key(sourceKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("The specified object does not have a ObjectLock configuration")
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  @Test
  @S3VerifiedFailure(year = 2025,
    reason = "S3 Object Lock makes it impossible to delete the object until the retention period is over.")
  fun testPutAndGetRetention(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3Client.createBucket {
      it.bucket(bucketName)
      it.objectLockEnabledForBucket(true)
    }
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      },
      RequestBody.fromFile(UPLOAD_FILE)
    )

    val retainUntilDate = Instant.now().plus(1, DAYS)
    s3Client.putObjectRetention {
      it.bucket(bucketName)
      it.key(sourceKey)
      it.retention(
        ObjectLockRetention.builder()
          .mode(ObjectLockRetentionMode.COMPLIANCE)
          .retainUntilDate(retainUntilDate)
          .build()
      )
    }

    s3Client.getObjectRetention {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.also {
      assertThat(it.retention().mode()).isEqualTo(ObjectLockRetentionMode.COMPLIANCE)
      //the returned date has MILLIS resolution, the local instant is in NANOS.
      assertThat(it.retention().retainUntilDate())
        .isCloseTo(
          retainUntilDate, within(1, MILLIS)
        )
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutInvalidRetentionUntilDate(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3Client.createBucket {
      it.bucket(bucketName)
      it.objectLockEnabledForBucket(true)
    }
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      },
      RequestBody.fromFile(UPLOAD_FILE)
    )

    val invalidRetainUntilDate = Instant.now().minus(1, DAYS)
    assertThatThrownBy {
      s3Client.putObjectRetention {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.retention(
          ObjectLockRetention.builder()
            .mode(ObjectLockRetentionMode.COMPLIANCE)
            .retainUntilDate(invalidRetainUntilDate)
            .build()
        )
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("The retain until date must be in the future!")
      .hasMessageContaining("Service: S3, Status Code: 400")
  }
}
