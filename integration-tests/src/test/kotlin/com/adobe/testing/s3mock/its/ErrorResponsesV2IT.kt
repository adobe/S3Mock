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
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.File

internal class ErrorResponsesV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    assertThatThrownBy {
      s3ClientV2.getObject {
        it.bucket(bucketName)
        it.key(NON_EXISTING_KEY)
      }
    }.isInstanceOf(
      NoSuchKeyException::class.java
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObject_noSuchKey_startingSlash(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    assertThatThrownBy {
      s3ClientV2.getObject {
        it.bucket(bucketName)
        it.key("/$NON_EXISTING_KEY")
      }
    }.isInstanceOf(
      NoSuchKeyException::class.java
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun putObject_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3ClientV2.putObject(
        {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun copyObjectToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      s3ClientV2.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteObject_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteObject {
        it.bucket(randomName)
        it.key(NON_EXISTING_KEY)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteObject_nonExistent_OK(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.deleteObject {
      it.bucket(bucketName)
      it.key(NON_EXISTING_KEY)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteObjects_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteObjects {
        it.bucket(randomName)
        it.delete {
          it.objects({
            it.key(NON_EXISTING_KEY)
          })
        }
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteBucket_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteBucket {
        it.bucket(randomName)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun multipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.createMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun listMultipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.listMultipartUploads {
        it.bucket(randomName)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun abortMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.abortMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId("uploadId")
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun uploadMultipart_invalidPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isNotEmpty

    val invalidPartNumber = 0
    assertThatThrownBy {
      s3ClientV2.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(invalidPartNumber)
        },
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(INVALID_PART_NUMBER)
  }

  companion object {
    private const val NON_EXISTING_KEY = "NoSuchKey.json"
    private const val NO_SUCH_KEY = "The specified key does not exist."
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
    private const val STATUS_CODE_404 = "Status Code: 404"
    private const val INVALID_PART_NUMBER = "Part number must be an integer between 1 and 10000, inclusive"
    private const val INVALID_PART = "Status Code: 400; Error Code: InvalidPart"
  }
}
