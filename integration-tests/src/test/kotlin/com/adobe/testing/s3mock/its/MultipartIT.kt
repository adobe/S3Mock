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

import com.adobe.testing.s3mock.S3Exception.Companion.PRECONDITION_FAILED
import com.adobe.testing.s3mock.util.DigestUtil
import com.adobe.testing.s3mock.util.DigestUtil.hexDigest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.InstanceOfAssertFactories
import org.assertj.core.util.Files
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.web.util.UriUtils
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ChecksumMode
import software.amazon.awssdk.services.s3.model.ChecksumType
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.ListPartsRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionException

internal class MultipartIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()
  private val s3AsyncClient: S3AsyncClient = createS3AsyncClient()
  private val s3CrtAsyncClient: S3AsyncClient = createS3CrtAsyncClient()
  private val transferManager: S3TransferManager = createTransferManager()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_asyncClient(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3CrtAsyncClient
      .putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
        },
        AsyncRequestBody.fromFile(UPLOAD_FILE),
      ).join()
      .also {
        assertThat(it.checksumCRC32()).isEqualTo(DigestUtil.checksumFor(UPLOAD_FILE_PATH, DefaultChecksumAlgorithm.CRC32))
      }

    s3AsyncClient.waiter().waitUntilObjectExists {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }

    val uploadDigest = hexDigest(UPLOAD_FILE)
    val downloadedDigest =
      s3Client
        .getObject {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }.use { response ->
          Files.newTemporaryFile().let {
            response.transferTo(it.outputStream())
            assertThat(it).hasSize(UPLOAD_FILE_LENGTH)
            assertThat(it).hasSameBinaryContentAs(UPLOAD_FILE)
            hexDigest(it)
          }
        }
    assertThat(uploadDigest).isEqualTo(downloadedDigest)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_transferManager(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    transferManager
      .uploadFile {
        it.putObjectRequest {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
        it.source(UPLOAD_FILE)
      }.completionFuture()
      .join()

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }

    val downloadFile = Files.newTemporaryFile()
    transferManager
      .downloadFile {
        it.getObjectRequest {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
        it.destination(downloadFile)
      }.also { download ->
        download.completionFuture().join().response().also {
          assertThat(it.contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
        }
      }
    assertThat(downloadFile.length()).isEqualTo(UPLOAD_FILE_LENGTH)
    assertThat(downloadFile).hasSameBinaryContentAs(UPLOAD_FILE)
  }

  /**
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_withUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf("key" to "value")
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.metadata(objectMetadata)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    s3Client.completeMultipartUpload {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
      it.uploadId(initiateMultipartUploadResult.uploadId())
      it.multipartUpload {
        it.parts({
          it.eTag(uploadPartResult.eTag())
          it.partNumber(1)
        })
      }
    }

    s3Client
      .getObject {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
      }.use {
        assertThat(it.response().metadata()).isEqualTo(objectMetadata)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_withChecksumType_COMPOSITE(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.checksumType(ChecksumType.COMPOSITE)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    val checksum =
      DigestUtil.checksumMultipart(
        listOf(UPLOAD_FILE_PATH),
        DefaultChecksumAlgorithm.CRC32,
      )

    s3Client
      .completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.checksumType(ChecksumType.COMPOSITE)
        it.multipartUpload {
          it.parts({
            it.eTag(uploadPartResult.eTag())
            it.partNumber(1)
            it.checksumCRC32(uploadPartResult.checksumCRC32())
          })
        }
      }.also {
        assertThat(it.checksumCRC32()).isEqualTo(checksum)
      }

    val etag = "\"${DigestUtil.hexDigestMultipart(listOf(UPLOAD_FILE_PATH))}\""
    s3Client
      .getObject {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(etag)
        assertThat(it.response().checksumCRC32()).isEqualTo(checksum)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_withChecksumType_throwsOn_DIFFERENT(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.checksumType(ChecksumType.COMPOSITE)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    assertThatThrownBy {
      s3Client
        .completeMultipartUpload {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(initiateMultipartUploadResult.uploadId())
          it.checksumType(ChecksumType.FULL_OBJECT) // intentionally different from creteMultipartUpload value
          it.multipartUpload {
            it.parts({
              it.eTag(uploadPartResult.eTag())
              it.partNumber(1)
              it.checksumCRC32(uploadPartResult.checksumCRC32())
            })
          }
        }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_withChecksumType_FULL_OBJECT(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC64_NVME)
          it.checksumType(ChecksumType.FULL_OBJECT)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC64_NVME)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    val checksum =
      DigestUtil.checksumFor(
        UPLOAD_FILE_PATH,
        DefaultChecksumAlgorithm.CRC64NVME,
      )

    s3Client
      .completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.checksumType(ChecksumType.FULL_OBJECT)
        it.multipartUpload {
          it.parts({
            it.eTag(uploadPartResult.eTag())
            it.partNumber(1)
            it.checksumCRC64NVME(uploadPartResult.checksumCRC64NVME())
          })
        }
      }.also {
        assertThat(it.checksumCRC64NVME()).isEqualTo(checksum)
      }

    val etag = "\"${DigestUtil.hexDigestMultipart(listOf(UPLOAD_FILE_PATH))}\""

    s3Client
      .getObject {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(etag)
        assertThat(it.response().checksumCRC64NVME()).isEqualTo(checksum)
      }
  }

  /**
   * Tests if a multipart upload with the last part being smaller than 5MB works.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, >5MB
    val randomBytes = randomBytes()
    val etag1 = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    // upload part 2, <5MB
    val etag2 =
      s3Client
        .uploadPart(
          {
            it.bucket(initiateMultipartUploadResult.bucket())
            it.key(initiateMultipartUploadResult.key())
            it.uploadId(uploadId)
            it.partNumber(2)
            it.contentLength(UPLOAD_FILE_LENGTH)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    val completeMultipartUpload =
      s3Client.completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.multipartUpload {
          it.parts(
            {
              it.eTag(etag1)
              it.partNumber(1)
            },
            {
              it.eTag(etag2)
              it.partNumber(2)
            },
          )
        }
      }

    val uploadFileBytes = readStreamIntoByteArray(UPLOAD_FILE.inputStream())
    val md5 = MessageDigest.getInstance("MD5")
    (md5.digest(randomBytes) + md5.digest(uploadFileBytes)).also {
      // verify special etag
      assertThat(completeMultipartUpload.eTag()).isEqualTo("\"${md5.digest(it).joinToString("") { "%02x".format(it) }}-2\"")
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.use {
        // verify content size
        assertThat(it.response().contentLength()).isEqualTo(randomBytes.size.toLong() + uploadFileBytes.size.toLong())
        // verify contents
        assertThat(readStreamIntoByteArray(it.buffered())).isEqualTo(concatByteArrays(randomBytes, uploadFileBytes))
        assertThat(it.response().metadata()).isEqualTo(objectMetadata)
      }

    assertThat(completeMultipartUpload.location())
      .isEqualTo("$serviceEndpoint/$bucketName/${UriUtils.encode(UPLOAD_FILE_NAME, StandardCharsets.UTF_8)}")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `multipartupload send checksum in create and complete`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(TEST_IMAGE_TIFF)
    // construct UPLOAD_FILE >5MB
    val tempFile =
      Files.newTemporaryFile().also { file ->
        (
          readStreamIntoByteArray(uploadFile.inputStream()) +
            readStreamIntoByteArray(uploadFile.inputStream()) +
            readStreamIntoByteArray(uploadFile.inputStream())
        ).inputStream()
          .use {
            it.copyTo(file.outputStream())
          }
      }

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(TEST_IMAGE_TIFF)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
        it.checksumType(ChecksumType.COMPOSITE)
      }
    assertThat(initiateMultipartUploadResult.checksumAlgorithm()).isEqualTo(ChecksumAlgorithm.CRC32)
    assertThat(initiateMultipartUploadResult.checksumType()).isEqualTo(ChecksumType.COMPOSITE)
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, <5MB
    val partResponse1 =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.partNumber(1)
          it.contentLength(tempFile.length())
        },
        RequestBody.fromFile(tempFile),
      )
    val etag1 = partResponse1.eTag()
    val checksum1 = partResponse1.checksumCRC32()
    // upload part 2, <5MB
    val partResponse2 =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.partNumber(2)
          it.contentLength(uploadFile.length())
        },
        RequestBody.fromFile(uploadFile),
      )
    val etag2 = partResponse2.eTag()
    val checksum2 = partResponse2.checksumCRC32()
    val localChecksum1 = DigestUtil.checksumFor(tempFile.toPath(), DefaultChecksumAlgorithm.CRC32)
    assertThat(checksum1).isEqualTo(localChecksum1)
    val localChecksum2 = DigestUtil.checksumFor(uploadFile.toPath(), DefaultChecksumAlgorithm.CRC32)
    assertThat(checksum2).isEqualTo(localChecksum2)

    val completeMultipartUpload =
      s3Client.completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.checksumType(ChecksumType.COMPOSITE)
        it.multipartUpload {
          it.parts(
            {
              it.eTag(etag1)
              it.partNumber(1)
              it.checksumCRC32(checksum1)
            },
            {
              it.eTag(etag2)
              it.partNumber(2)
              it.checksumCRC32(checksum2)
            },
          )
        }
      }

    val md5 = MessageDigest.getInstance("MD5")
    (md5.digest(tempFile.readBytes()) + md5.digest(readStreamIntoByteArray(uploadFile.inputStream()))).also {
      // verify special etag
      assertThat(completeMultipartUpload.eTag()).isEqualTo("\"${md5.digest(it).joinToString("") { "%02x".format(it) }}-2\"")
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(TEST_IMAGE_TIFF)
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        // verify content size
        assertThat(it.response().contentLength()).isEqualTo(tempFile.length() + uploadFile.length())
        // verify contents
        assertThat(readStreamIntoByteArray(it.buffered())).isEqualTo(tempFile.readBytes() + uploadFile.readBytes())
        assertThat(it.response().checksumCRC32()).isEqualTo("oGk6qg==-2")
      }

    assertThat(completeMultipartUpload.location())
      .isEqualTo("$serviceEndpoint/$bucketName/${UriUtils.encode(TEST_IMAGE_TIFF, StandardCharsets.UTF_8)}")

    // verify completeMultipartUpload is idempotent
    val completeMultipartUpload1 =
      s3Client.completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.checksumType(ChecksumType.COMPOSITE)
        it.multipartUpload {
          it.parts(
            {
              it.eTag(etag1)
              it.partNumber(1)
              it.checksumCRC32(checksum1)
            },
            {
              it.eTag(etag2)
              it.partNumber(2)
              it.checksumCRC32(checksum2)
            },
          )
        }
      }

    // for unknown reasons, a simple equals call fails on both objects.
    // assertThat(completeMultipartUpload).isEqualTo(completeMultipartUpload1)
    assertThat(completeMultipartUpload.location()).isEqualTo(completeMultipartUpload1.location())
    assertThat(completeMultipartUpload.bucket()).isEqualTo(completeMultipartUpload1.bucket())
    assertThat(completeMultipartUpload.key()).isEqualTo(completeMultipartUpload1.key())
    assertThat(completeMultipartUpload.eTag()).isEqualTo(completeMultipartUpload1.eTag())
    assertThat(completeMultipartUpload.checksumType()).isEqualTo(completeMultipartUpload1.checksumType())
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `multipartupload send checksum in create only`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(TEST_IMAGE_TIFF)
    // construct uploadfile >5MB
    val tempFile =
      Files.newTemporaryFile().also { file ->
        (
          readStreamIntoByteArray(uploadFile.inputStream()) +
            readStreamIntoByteArray(uploadFile.inputStream()) +
            readStreamIntoByteArray(uploadFile.inputStream())
        ).inputStream()
          .use {
            it.copyTo(file.outputStream())
          }
      }

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(TEST_IMAGE_TIFF)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, <5MB
    val partResponse1 =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.partNumber(1)
          it.contentLength(tempFile.length())
        },
        RequestBody.fromFile(tempFile),
      )
    val etag1 = partResponse1.eTag()
    val checksum1 = partResponse1.checksumCRC32()
    // upload part 2, <5MB
    val partResponse2 =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
          it.partNumber(2)
          it.contentLength(uploadFile.length())
        },
        RequestBody.fromFile(uploadFile),
      )
    val etag2 = partResponse2.eTag()
    val checksum2 = partResponse2.checksumCRC32()
    val localChecksum1 = DigestUtil.checksumFor(tempFile.toPath(), DefaultChecksumAlgorithm.CRC32)
    assertThat(checksum1).isEqualTo(localChecksum1)
    val localChecksum2 = DigestUtil.checksumFor(uploadFile.toPath(), DefaultChecksumAlgorithm.CRC32)
    assertThat(checksum2).isEqualTo(localChecksum2)

    assertThatThrownBy {
      s3Client.completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
        it.multipartUpload {
          it.parts(
            {
              it.eTag(etag1)
              it.partNumber(1)
            },
            {
              it.eTag(etag2)
              it.partNumber(2)
            },
          )
        }
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining(
        "The upload was created using a crc32 checksum. The complete request must include the " +
          "checksum for each part. It was missing for part 1 in the request.",
      )
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testUploadPart_checksumAlgorithm_initiate(
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)
    val expectedChecksum = DigestUtil.checksumFor(UPLOAD_FILE_PATH, checksumAlgorithm)
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(checksumAlgorithm.toAlgorithm())
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    s3Client
      .uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksumAlgorithm(checksumAlgorithm.toAlgorithm())
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH).build()
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).also {
        val actualChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(actualChecksum).isNotBlank
        assertThat(actualChecksum).isEqualTo(expectedChecksum)
      }
    s3Client.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId)
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testUploadPart_checksumAlgorithm_complete(
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    s3Client.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(UPLOAD_FILE_LENGTH).build()
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    assertThatThrownBy {
      s3Client.completeMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(uploadId)
        it.checksumType(ChecksumType.COMPOSITE)
        it.checksum("WRONG CHECKSUM", checksumAlgorithm.toAlgorithm())
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testMultipartUpload_checksum(
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)
    val expectedChecksum = DigestUtil.checksumFor(UPLOAD_FILE_PATH, checksumAlgorithm)
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(checksumAlgorithm.toAlgorithm())
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    s3Client
      .uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksum(expectedChecksum, checksumAlgorithm.toAlgorithm())
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH).build()
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).also {
        val actualChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(actualChecksum).isNotBlank
        assertThat(actualChecksum).isEqualTo(expectedChecksum)
      }
    s3Client.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId).build()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testMultipartUpload_wrongChecksum(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val expectedChecksum = "wrongChecksum"
    val checksumAlgorithm = ChecksumAlgorithm.SHA1
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThatThrownBy {
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksum(expectedChecksum, checksumAlgorithm)
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH).build()
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Value for x-amz-checksum-sha1 header is invalid.")
  }

  private fun UploadPartRequest.Builder.checksum(
    checksum: String,
    checksumAlgorithm: ChecksumAlgorithm,
  ): UploadPartRequest.Builder =
    when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> checksumSHA1(checksum)
      ChecksumAlgorithm.SHA256 -> checksumSHA256(checksum)
      ChecksumAlgorithm.CRC32 -> checksumCRC32(checksum)
      ChecksumAlgorithm.CRC32_C -> checksumCRC32C(checksum)
      ChecksumAlgorithm.CRC64_NVME -> checksumCRC64NVME(checksum)
      else -> error("Unknown checksum algorithm")
    }

  private fun CompleteMultipartUploadRequest.Builder.checksum(
    checksum: String,
    checksumAlgorithm: ChecksumAlgorithm,
  ): CompleteMultipartUploadRequest.Builder =
    when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> checksumSHA1(checksum)
      ChecksumAlgorithm.SHA256 -> checksumSHA256(checksum)
      ChecksumAlgorithm.CRC32 -> checksumCRC32(checksum)
      ChecksumAlgorithm.CRC32_C -> checksumCRC32C(checksum)
      ChecksumAlgorithm.CRC64_NVME -> checksumCRC64NVME(checksum)
      else -> error("Unknown checksum algorithm")
    }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list parts lists all uploaded parts`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf("key" to "value")
    val md5 = MessageDigest.getInstance("MD5")
    val hash = UPLOAD_FILE.inputStream().use { md5.digest(it.readBytes()).joinToString("") { "%02x".format(it) } }
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val key = initiateMultipartUploadResult.key()

    s3Client.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(key)
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(UPLOAD_FILE_LENGTH)
        // .lastPart(true)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(key)
        it.uploadId(uploadId)
        it.partNumber(2)
        it.contentLength(UPLOAD_FILE_LENGTH)
        // .lastPart(true)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    val partListing =
      s3Client
        .listParts {
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
        }.also {
          assertThat(it.parts()).hasSize(2)
        }

    partListing.parts()[0].also {
      assertThat(it.eTag()).isEqualTo("\"$hash\"")
      assertThat(it.partNumber()).isEqualTo(1)
      assertThat(it.lastModified()).isExactlyInstanceOf(Instant::class.java)
    }

    partListing.parts()[1].also {
      assertThat(it.eTag()).isEqualTo("\"$hash\"")
      assertThat(it.partNumber()).isEqualTo(2)
      assertThat(it.lastModified()).isExactlyInstanceOf(Instant::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list parts lists uploaded parts matching parameters`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf("key" to "value")
    val md5 = MessageDigest.getInstance("MD5")
    val hash = UPLOAD_FILE.inputStream().use { md5.digest(it.readBytes()).joinToString("") { "%02x".format(it) } }
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val key = initiateMultipartUploadResult.key()

    for (i in 1..10) {
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(key)
          it.uploadId(uploadId)
          it.partNumber(i)
          it.contentLength(UPLOAD_FILE_LENGTH)
          // .lastPart(true)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }

    val partListing1 =
      s3Client
        .listParts {
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
          it.maxParts(5)
        }.also {
          assertThat(it.parts()).hasSize(5)
          assertThat(it.nextPartNumberMarker()).isEqualTo(5)
          assertThat(it.isTruncated).isTrue
        }

    val partListing2 =
      s3Client
        .listParts {
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
          it.maxParts(5)
          it.partNumberMarker(partListing1.nextPartNumberMarker())
        }.also {
          assertThat(it.parts()).hasSize(5)
          // assertThat(it.nextPartNumberMarker()).isNull()
          assertThat(it.isTruncated).isFalse
        }

    partListing1.parts()[0].also {
      assertThat(it.eTag()).isEqualTo("\"$hash\"")
      assertThat(it.partNumber()).isEqualTo(1)
      assertThat(it.lastModified()).isExactlyInstanceOf(Instant::class.java)
    }

    partListing2.parts()[0].also {
      assertThat(it.eTag()).isEqualTo("\"$hash\"")
      assertThat(it.partNumber()).isEqualTo(6)
      assertThat(it.lastModified()).isExactlyInstanceOf(Instant::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list parts is empty if no parts were uploaded`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isEmpty()
    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }.uploadId()

    s3Client
      .listParts {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(uploadId)
      }.also {
        assertThat(it.parts()).isEmpty()
        assertThat(it.bucket()).isEqualTo(bucketName)
        assertThat(it.uploadId()).isEqualTo(uploadId)
        assertThat(SdkHttpUtils.urlDecode(it.key())).isEqualTo(UPLOAD_FILE_NAME)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list parts for unknown uploadId throws exception`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.listParts {
        it.bucket(bucketName)
        it.key("NON_EXISTENT_KEY")
        it.uploadId(UUID.randomUUID().toString())
      }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list MultipartUploads returns OK`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isEmpty()
    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }.uploadId()

    s3Client
      .listMultipartUploads {
        it.bucket(bucketName)
      }.also { listing ->
        assertThat(listing.uploads()).isNotEmpty
        assertThat(listing.bucket()).isEqualTo(bucketName)
        assertThat(listing.uploads()).hasSize(1)

        listing
          .uploads()[0]
          .also {
            assertThat(it.uploadId()).isEqualTo(uploadId)
            assertThat(it.key()).isEqualTo(UPLOAD_FILE_NAME)
          }
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list MultipartUploads by prefix returns OK`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.createMultipartUpload {
      it.bucket(bucketName)
      it.key("key1")
    }
    s3Client.createMultipartUpload {
      it.bucket(bucketName)
      it.key("key2")
    }

    val listing =
      s3Client.listMultipartUploads {
        it.bucket(bucketName)
        it.prefix("key2")
      }
    assertThat(listing.uploads()).hasSize(1)
    assertThat(listing.uploads()[0].key()).isEqualTo("key2")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list MultipartUploads by bucket returns OK`(testInfo: TestInfo) {
    // create multipart upload 1
    val bucketName1 =
      givenBucket(testInfo)
        .also { name ->
          s3Client.createMultipartUpload {
            it.bucket(name)
            it.key("key1")
          }
        }

    // create multipart upload 2
    val bucketName2 =
      givenBucket()
        .also { name ->
          s3Client.createMultipartUpload {
            it.bucket(name)
            it.key("key2")
          }
        }

    // assert multipart upload 1
    s3Client
      .listMultipartUploads {
        it.bucket(bucketName1)
      }.also {
        assertThat(it.uploads()).hasSize(1)
        assertThat(it.uploads()[0].key()).isEqualTo("key1")
      }

    // assert multipart upload 2
    s3Client
      .listMultipartUploads {
        it.bucket(bucketName2)
      }.also {
        assertThat(it.uploads()).hasSize(1)
        assertThat(it.uploads()[0].key()).isEqualTo("key2")
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `list MultipartUploads limited by max-uploads returns OK`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    for (i in 1..10) {
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key("key$i")
      }
    }
    val uploads1 =
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
          it.maxUploads(5)
        }.also {
          assertThat(it.uploads()).hasSize(5)
          assertThat(it.nextUploadIdMarker()).isNotNull
          assertThat(it.nextKeyMarker()).isNotNull
        }

    s3Client
      .listMultipartUploads {
        it.bucket(bucketName)
        it.uploadIdMarker(uploads1.nextUploadIdMarker())
        it.keyMarker(uploads1.nextKeyMarker())
      }.also {
        assertThat(it.uploads()).hasSize(5)
        assertThat(it.uploads()[0].key()).isEqualTo("key5")
      }
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testAbortMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.hasUploads(),
    ).isFalse

    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }.uploadId()
    val randomBytes = randomBytes()

    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.hasUploads(),
    ).isTrue

    s3Client
      .listParts {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(uploadId)
      }.parts()
      .also {
        assertThat(it).hasSize(1)
        assertThat(it[0].eTag()).isEqualTo(partETag)
      }

    s3Client.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId)
    }
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.hasUploads(),
    ).isFalse

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3Client.listParts {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(uploadId)
      }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchUpload")
  }

  /**
   * Tests if the parts specified in CompleteUploadRequest are adhered
   * irrespective of the number of parts uploaded before.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testCompleteMultipartUpload_partLeftOut(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = randomName
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isEmpty()

    // Initiate upload
    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(key)
        }.uploadId()

    // Upload 3 parts
    val randomBytes1 = randomBytes()
    val partETag1 = uploadPart(bucketName, key, uploadId, 1, randomBytes1)
    val randomBytes2 = randomBytes()
    uploadPart(bucketName, key, uploadId, 2, randomBytes2) // ignore output in this test.
    val randomBytes3 = randomBytes()
    val partETag3 = uploadPart(bucketName, key, uploadId, 3, randomBytes3)

    // Try to complete with these parts
    val result =
      s3Client.completeMultipartUpload {
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
        it.multipartUpload {
          it.parts(
            {
              it.eTag(partETag1)
              it.partNumber(1)
            },
            {
              it.eTag(partETag3)
              it.partNumber(3)
            },
          )
        }
      }

    val md5 = MessageDigest.getInstance("MD5")
    // Verify only 1st and 3rd counts
    (md5.digest(randomBytes1) + md5.digest(randomBytes3)).also {
      // verify special etag
      assertThat(result.eTag()).isEqualTo("\"${md5.digest(it).joinToString("") { "%02x".format(it) }}-2\"")
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        // verify content size
        assertThat(it.response().contentLength()).isEqualTo(randomBytes1.size.toLong() + randomBytes3.size)
        // verify contents
        assertThat(readStreamIntoByteArray(it.buffered())).isEqualTo(concatByteArrays(randomBytes1, randomBytes3))
      }
  }

  /**
   * Tests that uploaded parts can be listed regardless if the MultipartUpload was completed or
   * aborted.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testListParts_completeAndAbort(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = randomName
    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isEmpty()

    // Initiate upload
    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(key)
        }.uploadId()

    // Upload part
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, key, uploadId, 1, randomBytes)

    // List parts, make sure we find part 1
    s3Client
      .listParts {
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
      }.parts()
      .also {
        assertThat(it).hasSize(1)
        assertThat(it[0].eTag()).isEqualTo(partETag)
      }

    // Complete
    s3Client.completeMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
      it.multipartUpload {
        it.parts(
          {
            it.eTag(partETag)
            it.partNumber(1)
          },
        )
      }
    }

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3Client.listParts {
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
      }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchUpload")
  }

  /**
   * Upload two objects, copy as parts without length, complete multipart.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun shouldCopyPartsAndComplete(testInfo: TestInfo) {
    // Initiate upload
    val bucketName2 = givenBucket()
    val multipartUploadKey = UUID.randomUUID().toString()

    val uploadId =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName2)
          it.key(multipartUploadKey)
        }.uploadId()
    val parts: MutableList<CompletedPart> = ArrayList()

    // bucket for test data
    val bucketName1 = givenBucket(testInfo)

    // create two objects, initiate copy part with full object length
    val sourceKeys = arrayOf(UUID.randomUUID().toString(), UUID.randomUUID().toString())
    val allRandomBytes: MutableList<ByteArray> = ArrayList()
    for (i in sourceKeys.indices) {
      val key = sourceKeys[i]
      val partNumber = i + 1
      val randomBytes = randomBytes()
      val metadata1 =
        HashMap<String, String>().apply {
          this["contentLength"] = randomBytes.size.toString()
        }
      ByteArrayInputStream(randomBytes).use { inputStream ->
        s3Client.putObject(
          {
            it.bucket(bucketName1)
            it.key(key)
            it.metadata(metadata1)
          },
          RequestBody.fromInputStream(inputStream, randomBytes.size.toLong()),
        )
      }

      s3Client
        .uploadPartCopy {
          it.partNumber(partNumber)
          it.uploadId(uploadId)
          it.destinationBucket(bucketName2)
          it.destinationKey(multipartUploadKey)
          it.sourceKey(key)
          it.sourceBucket(bucketName1)
        }.also {
          val etag = it.copyPartResult().eTag()
          parts.add(
            CompletedPart
              .builder()
              .eTag(etag)
              .partNumber(partNumber)
              .build(),
          )
          allRandomBytes.add(randomBytes)
        }
    }
    assertThat(allRandomBytes).hasSize(2)

    // Complete with parts
    val result =
      s3Client.completeMultipartUpload {
        it.bucket(bucketName2)
        it.key(multipartUploadKey)
        it.uploadId(uploadId)
        it.multipartUpload {
          it.parts(parts)
        }
      }
    val md5 = MessageDigest.getInstance("MD5")
    // Verify parts
    (md5.digest(allRandomBytes[0]) + md5.digest(allRandomBytes[1])).also {
      // verify etag
      assertThat(result.eTag()).isEqualTo("\"${md5.digest(it).joinToString("") { "%02x".format(it) }}-2\"")
    }

    s3Client
      .getObject {
        it.bucket(bucketName2)
        it.key(multipartUploadKey)
      }.use {
        // verify content size
        assertThat(it.response().contentLength()).isEqualTo(allRandomBytes[0].size.toLong() + allRandomBytes[1].size)

        // verify contents
        assertThat(readStreamIntoByteArray(it.buffered()))
          .isEqualTo(concatByteArrays(allRandomBytes[0], allRandomBytes[1]))
      }
  }

  /**
   * Puts an Object; Copies part of that object to a new bucket;
   * Requests parts for the uploadId; compares etag of upload response and parts list.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun shouldCopyObjectPart(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val objectMetadata = mapOf(Pair("key", "value"))

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
      }.also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  /**
   * Tries to copy part of a non-existing object to a new bucket.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun shouldThrowNoSuchKeyOnCopyObjectPartForNonExistingKey(testInfo: TestInfo) {
    val sourceKey = "NON_EXISTENT_KEY"
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
        it.metadata(objectMetadata)
      }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-5")
      }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchKey")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun `UploadPartCopy succeeds with copy-source-if-match=true`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfMatch(matchingEtag)
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts(
        ListPartsRequest
          .builder()
          .bucket(initiateMultipartUploadResult.bucket())
          .key(initiateMultipartUploadResult.key())
          .uploadId(initiateMultipartUploadResult.uploadId())
          .build(),
      ).also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy fails with copy-source-if-match=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-$UPLOAD_FILE_LENGTH")
        it.copySourceIfMatch(noneMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun `UploadPartCopy succeeds with copy-source-if-match=true and copy-source-if-unmodified-since=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val now = Instant.now().plusSeconds(60)

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfMatch(matchingEtag)
        it.copySourceIfUnmodifiedSince(now)
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts(
        ListPartsRequest
          .builder()
          .bucket(initiateMultipartUploadResult.bucket())
          .key(initiateMultipartUploadResult.key())
          .uploadId(initiateMultipartUploadResult.uploadId())
          .build(),
      ).also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy succeeds with copy-source-if-none-match=true`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfNoneMatch(noneMatchingEtag)
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
      }.also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy fails with copy-source-if-none-match=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-$UPLOAD_FILE_LENGTH")
        it.copySourceIfNoneMatch(matchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy fails with copy-source-if-none-match=false and copy-source-if-modified-since=true`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-$UPLOAD_FILE_LENGTH")
        it.copySourceIfNoneMatch(matchingEtag)
        it.copySourceIfModifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy succeeds with copy-source-if-modified-since=true`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfModifiedSince(now)
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
      }.also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy fails with copy-source-if-modified-since=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }

    val now = Instant.now().plusSeconds(60)

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfModifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy succeeds with copy-source-if-unmodified-since=true`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val now = Instant.now().plusSeconds(60)

    val result =
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-${UPLOAD_FILE_LENGTH - 1}")
        it.copySourceIfUnmodifiedSince(now)
      }
    val etag = result.copyPartResult().eTag()

    s3Client
      .listParts {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(initiateMultipartUploadResult.uploadId())
      }.also {
        assertThat(it.parts()).hasSize(1)
        assertThat(it.parts()[0].eTag()).isEqualTo(etag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `UploadPartCopy fails with copy-source-if-unmodified-since=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val initiateMultipartUploadResult =
      s3Client.createMultipartUpload {
        it.bucket(destinationBucket)
        it.key(destinationKey)
      }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3Client.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-$UPLOAD_FILE_LENGTH")
        it.copySourceIfUnmodifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun createMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3Client.createMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun listMultipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listMultipartUploads {
        it.bucket(randomName)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun abortMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3Client.abortMultipartUpload {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(UUID.randomUUID().toString())
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun transferManagerUpload_noSuchSourceBucket() {
    assertThatThrownBy {
      transferManager
        .upload {
          it.putObjectRequest {
            it.bucket(randomName)
            it.key(UPLOAD_FILE_NAME)
          }
          it.requestBody(AsyncRequestBody.fromFile(UPLOAD_FILE))
        }.completionFuture()
        .join()
    }.isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun transferManagerCopy_noSuchDestinationBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (_, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      transferManager
        .copy {
          it.copyObjectRequest {
            it.sourceBucket(randomName)
            it.sourceKey(sourceKey)
            it.destinationBucket(destinationBucketName)
            it.destinationKey(destinationKey)
          }
        }.completionFuture()
        .join()
    }.isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchKeyException::class.java)
    // TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
    // .hasMessageContaining(NO_SUCH_KEY)
    // TODO: not sure why AWS SDK v2 does not return the correct exception here, S3Mock returns the correct error message.
    // .hasCauseInstanceOf(NoSuchBucketException::class.java)
    // .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun transferManagerCopy_noSuchSourceKey(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      transferManager
        .copy {
          it.copyObjectRequest {
            it.sourceBucket(bucketName)
            it.sourceKey(randomName)
            it.destinationBucket(destinationBucketName)
            it.destinationKey(destinationKey)
          }
        }.completionFuture()
        .join()
    }.isInstanceOf(CompletionException::class.java)
      .hasCauseInstanceOf(NoSuchKeyException::class.java)
    // TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
    // .hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun uploadMultipart_invalidPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
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
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(INVALID_PART_NUMBER)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun completeMultipartUpload_nonExistingPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isNotEmpty

    val eTag =
      s3Client
        .uploadPart(
          {
            it.bucket(initiateMultipartUploadResult.bucket())
            it.key(initiateMultipartUploadResult.key())
            it.uploadId(uploadId)
            it.partNumber(1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    val invalidPartNumber = 10
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
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(INVALID_PART)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `CompleteMultipart fails with if-none-match=true`(testInfo: TestInfo) {
    val (bucketName, response) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThat(
      s3Client
        .listMultipartUploads {
          it.bucket(bucketName)
        }.uploads(),
    ).isNotEmpty

    val eTag =
      s3Client
        .uploadPart(
          {
            it.bucket(initiateMultipartUploadResult.bucket())
            it.key(initiateMultipartUploadResult.key())
            it.uploadId(uploadId)
            it.partNumber(1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    assertThatThrownBy {
      s3Client.completeMultipartUpload {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.ifNoneMatch("*")
        it.multipartUpload {
          it.parts({
            it.eTag(eTag)
            it.partNumber(1)
          })
        }
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  private fun uploadPart(
    bucketName: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    randomBytes: ByteArray,
  ): String {
    ByteArrayInputStream(randomBytes).use { inputStream ->
      return s3Client
        .uploadPart(
          {
            it.bucket(bucketName)
            it.key(key)
            it.uploadId(uploadId)
            it.partNumber(partNumber)
            it.contentLength(randomBytes.size.toLong())
          },
          RequestBody.fromInputStream(inputStream, randomBytes.size.toLong()),
        ).eTag()
    }
  }

  companion object {
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
    private const val INVALID_PART_NUMBER = "Part number must be an integer between 1 and 10000, inclusive"
    private const val INVALID_PART =
      "One or more of the specified parts could not be found.  " +
        "The part may not have been uploaded, or the specified entity tag may not match the part's entity tag."
  }
}
