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
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import java.io.File
import java.util.concurrent.CompletionException

internal class ErrorResponsesV2IT : S3TestBase() {

  private val s3Client: S3Client = createS3Client()
  private val transferManager = createTransferManager()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.getObject {
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
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.getObject {
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
      s3Client.putObject(
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
  @S3VerifiedTodo
  fun putObjectEncrypted_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
          it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
          it.ssekmsKeyId(TEST_ENC_KEY_ID)
        },
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun headObject_noSuchBucket() {
    assertThatThrownBy {
      s3Client.headObject {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
        }
    }
      //TODO: not sure why AWS SDK v2 does not return the correct exception here, S3Mock returns the correct error message.
      .isInstanceOf(NoSuchKeyException::class.java)
      //.isInstanceOf(NoSuchBucketException::class.java)
      //.hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun headObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.headObject {
          it.bucket(bucketName)
          it.key(NON_EXISTING_KEY)
        }
    }
      .isInstanceOf(NoSuchKeyException::class.java)
      //TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
      //.hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun copyObjectToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      s3Client.copyObject {
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
      s3Client.deleteObject {
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
    val bucketName = givenBucket(testInfo)

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(NON_EXISTING_KEY)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteObjects_noSuchBucket() {
    assertThatThrownBy {
      s3Client.deleteObjects {
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
      s3Client.deleteBucket {
        it.bucket(randomName)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun createMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3Client.createMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun listObjects_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listObjects {
        it.bucket(randomName)
        it.prefix(UPLOAD_FILE_NAME)
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun listMultipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listMultipartUploads {
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
      s3Client.abortMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId("uploadId")
      }
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun transferManagerUpload_noSuchSourceBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy {
      transferManager.upload {
        it.putObjectRequest {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
        }
        it.requestBody(AsyncRequestBody.fromFile(uploadFile))
      }.completionFuture().join()
    }
      .isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun transferManagerCopy_noSuchDestinationBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      transferManager.copy {
        it.copyObjectRequest {
          it.sourceBucket(randomName)
          it.sourceKey(UPLOAD_FILE_NAME)
          it.destinationBucket(destinationBucketName)
          it.destinationKey(destinationKey)
        }
      }.completionFuture().join()
    }
      .isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchKeyException::class.java)
      //TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
      //.hasMessageContaining(NO_SUCH_KEY)
      //TODO: not sure why AWS SDK v2 does not return the correct exception here, S3Mock returns the correct error message.
      //.hasCauseInstanceOf(NoSuchBucketException::class.java)
      //.hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun transferManagerCopy_noSuchSourceKey(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      transferManager.copy {
        it.copyObjectRequest {
          it.sourceBucket(bucketName)
          it.sourceKey(randomName)
          it.destinationBucket(destinationBucketName)
          it.destinationKey(destinationKey)
        }
      }.completionFuture().join()
    }
      .isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchKeyException::class.java)
      //TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
      //.hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun uploadMultipart_invalidPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3Client.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isNotEmpty

    val invalidPartNumber = 0
    assertThatThrownBy {
      s3Client.uploadPart(
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

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun completeMultipartUpload_nonExistingPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3Client.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isNotEmpty

    val eTag = s3Client.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(1)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    val invalidPartNumber = 0
    assertThatThrownBy {
      s3Client.completeMultipartUpload {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.multipartUpload {
            it.parts({
              it.eTag(eTag)
              it.partNumber(invalidPartNumber)
            })
          }
        }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(INVALID_PART)
  }

  companion object {
    private const val NON_EXISTING_KEY = "NoSuchKey.json"
    private const val NO_SUCH_KEY = "The specified key does not exist."
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
    private const val STATUS_CODE_404 = "Status Code: 404"
    private const val INVALID_PART_NUMBER = "Part number must be an integer between 1 and 10000, inclusive"
    private const val INVALID_PART = "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tagSet might not have matched the part's entity tagSet."
  }
}
