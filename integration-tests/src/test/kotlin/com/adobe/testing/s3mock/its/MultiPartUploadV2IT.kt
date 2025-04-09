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

import com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED
import com.adobe.testing.s3mock.util.DigestUtil
import com.adobe.testing.s3mock.util.DigestUtil.hexDigest
import org.apache.commons.codec.digest.DigestUtils
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
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.checksums.Algorithm.CRC32
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ChecksumMode
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.ListPartsRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files.newOutputStream
import java.time.Instant
import java.util.UUID


internal class MultiPartUploadV2IT : S3TestBase() {
  private val s3ClientV2: S3Client = createS3ClientV2()
  private val s3AsyncClientV2: S3AsyncClient = createS3AsyncClientV2()
  private val s3CrtAsyncClientV2: S3AsyncClient = createS3CrtAsyncClientV2()
  private val autoS3CrtAsyncClientV2: S3AsyncClient = createAutoS3CrtAsyncClientV2()
  private val transferManagerV2: S3TransferManager = createTransferManagerV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload_asyncClient(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3CrtAsyncClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key(uploadFile.name)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
      },
      AsyncRequestBody.fromFile(uploadFile)
    ).join().also {
      assertThat(it.checksumCRC32()).isEqualTo(DigestUtil.checksumFor(uploadFile.toPath(), CRC32))
    }

    s3AsyncClientV2.waiter().waitUntilObjectExists {
      it.bucket(bucketName)
      it.key(uploadFile.name)
    }

    val uploadDigest = hexDigest(uploadFile)
    val downloadedDigest = s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(uploadFile.name)
    }.use { response ->
      Files.newTemporaryFile().let {
        response.transferTo(newOutputStream(it.toPath()))
        assertThat(it).hasSize(uploadFile.length())
        assertThat(it).hasSameBinaryContentAs(uploadFile)
        hexDigest(it)
      }
    }
    assertThat(uploadDigest).isEqualTo(downloadedDigest)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload_transferManager(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    transferManagerV2
      .uploadFile {
        it.putObjectRequest {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
        it.source(uploadFile)
      }.completionFuture().join()

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }

    val downloadFile = Files.newTemporaryFile()
    transferManagerV2.downloadFile {
      it.getObjectRequest {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
      it.destination(downloadFile)
    }.also { download ->
      download.completionFuture().join().response().also {
        assertThat(it.contentLength()).isEqualTo(uploadFile.length())
      }
    }
    assertThat(downloadFile.length()).isEqualTo(uploadFile.length())
    assertThat(downloadFile).hasSameBinaryContentAs(uploadFile)
  }

  /**
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload_withUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult = s3ClientV2.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(uploadFile.length())
        //it.lastPart(true)
      },
      RequestBody.fromFile(uploadFile),
    )

    s3ClientV2.completeMultipartUpload {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
      it.uploadId(initiateMultipartUploadResult.uploadId())
      it.multipartUpload {
        it.parts({
          it.eTag(uploadPartResult.eTag())
          it.partNumber(1)
        }
        )
      }
    }

    s3ClientV2.getObject {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
    }.use {
      assertThat(it.response().metadata()).isEqualTo(objectMetadata)
    }
  }

  /**
   * Tests if a multipart upload with the last part being smaller than 5MB works.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.metadata(objectMetadata)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, >5MB
    val randomBytes = randomBytes()
    val etag1 = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    // upload part 2, <5MB
    val etag2 = s3ClientV2.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(2)
        it.contentLength(uploadFile.length())
        //it.lastPart(true)
      },
      RequestBody.fromFile(uploadFile),
    ).eTag()

    val completeMultipartUpload = s3ClientV2.completeMultipartUpload {
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
          }
        )
      }
    }

    val uploadFileBytes = readStreamIntoByteArray(uploadFile.inputStream())

    (DigestUtils.md5(randomBytes) + DigestUtils.md5(uploadFileBytes)).also {
      // verify special etag
      assertThat(completeMultipartUpload.eTag()).isEqualTo("\"${DigestUtils.md5Hex(it)}-2\"")
    }

    s3ClientV2.getObject {
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
      .isEqualTo("${serviceEndpoint}/$bucketName/${UriUtils.encode(UPLOAD_FILE_NAME, StandardCharsets.UTF_8)}")
  }


  /**
   * Tests if a multipart upload with the last part being smaller than 5MB works.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload_checksum(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(TEST_IMAGE_TIFF)
    //construct uploadfile >5MB
    val tempFile = Files.newTemporaryFile().also {
      (readStreamIntoByteArray(uploadFile.inputStream()) +
        readStreamIntoByteArray(uploadFile.inputStream()) +
        readStreamIntoByteArray(uploadFile.inputStream()))
        .inputStream()
        .copyTo(it.outputStream())
    }

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
        it.bucket(bucketName)
        it.key(TEST_IMAGE_TIFF)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, <5MB
    val partResponse1 = s3ClientV2.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
        it.partNumber(1)
        it.contentLength(tempFile.length())
      },
      //.lastPart(true)
      RequestBody.fromFile(tempFile),
    )
    val etag1 = partResponse1.eTag()
    val checksum1 = partResponse1.checksumCRC32()
    // upload part 2, <5MB
    val partResponse2 = s3ClientV2.uploadPart({
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
        it.partNumber(2)
        it.contentLength(uploadFile.length())
      },
      //.lastPart(true)
      RequestBody.fromFile(uploadFile),
    )
    val etag2 = partResponse2.eTag()
    val checksum2 = partResponse2.checksumCRC32()
    val localChecksum1 = DigestUtil.checksumFor(tempFile.toPath(), CRC32)
    assertThat(checksum1).isEqualTo(localChecksum1)
    val localChecksum2 = DigestUtil.checksumFor(uploadFile.toPath(), CRC32)
    assertThat(checksum2).isEqualTo(localChecksum2)

    val completeMultipartUpload = s3ClientV2.completeMultipartUpload {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
      it.uploadId(initiateMultipartUploadResult.uploadId())
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
          }
        )
      }
    }

    (DigestUtils.md5(tempFile.readBytes()) + DigestUtils.md5(readStreamIntoByteArray(uploadFile.inputStream()))).also {
      // verify special etag
      assertThat(completeMultipartUpload.eTag()).isEqualTo("\"${DigestUtils.md5Hex(it)}-2\"")
    }

    s3ClientV2.getObject {
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
      .isEqualTo("${serviceEndpoint}/$bucketName/${UriUtils.encode(TEST_IMAGE_TIFF, StandardCharsets.UTF_8)}")
  }


  @S3VerifiedSuccess(year = 2024)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testUploadPart_checksumAlgorithm(checksumAlgorithm: ChecksumAlgorithm, testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.checksumAlgorithm(checksumAlgorithm)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    s3ClientV2.uploadPart({
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.checksumAlgorithm(checksumAlgorithm)
        it.partNumber(1)
        it.contentLength(uploadFile.length()).build()
        //.lastPart(true)
      },
      RequestBody.fromFile(uploadFile),
    ).also {
      val actualChecksum = it.checksum(checksumAlgorithm)
      assertThat(actualChecksum).isNotBlank
      assertThat(actualChecksum).isEqualTo(expectedChecksum)
    }
    s3ClientV2.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId)
    }
  }

  @S3VerifiedSuccess(year = 2024)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testMultipartUpload_checksum(checksumAlgorithm: ChecksumAlgorithm, testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.checksumAlgorithm(checksumAlgorithm)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    s3ClientV2.uploadPart({
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.checksum(expectedChecksum, checksumAlgorithm)
        it.partNumber(1)
        it.contentLength(uploadFile.length()).build()
        //.lastPart(true)
      },
      RequestBody.fromFile(uploadFile),
    ).also {
      val actualChecksum = it.checksum(checksumAlgorithm)
      assertThat(actualChecksum).isNotBlank
      assertThat(actualChecksum).isEqualTo(expectedChecksum)
    }
    s3ClientV2.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId).build()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload_wrongChecksum(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "wrongChecksum"
    val checksumAlgorithm = ChecksumAlgorithm.SHA1
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    assertThatThrownBy {
      s3ClientV2.uploadPart({
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.checksum(expectedChecksum, checksumAlgorithm)
          it.partNumber(1)
          it.contentLength(uploadFile.length()).build()
          //it.lastPart(true)
        },
        RequestBody.fromFile(uploadFile),
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Value for x-amz-checksum-sha1 header is invalid.")
  }

  private fun UploadPartRequest.Builder.checksum(
    checksum: String,
    checksumAlgorithm: ChecksumAlgorithm
  ): UploadPartRequest.Builder =
    when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> this.checksumSHA1(checksum)
      ChecksumAlgorithm.SHA256 -> this.checksumSHA256(checksum)
      ChecksumAlgorithm.CRC32 -> this.checksumCRC32(checksum)
      ChecksumAlgorithm.CRC32_C -> this.checksumCRC32C(checksum)
      else -> error("Unknown checksum algorithm")
    }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testInitiateMultipartAndRetrieveParts(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val hash = DigestUtils.md5Hex(FileInputStream(uploadFile))
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()
    val key = initiateMultipartUploadResult.key()

    s3ClientV2.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(key)
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(uploadFile.length())
        //.lastPart(true)
      },
      RequestBody.fromFile(uploadFile),
    )

    val partListing = s3ClientV2.listParts {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
    }.also {
      assertThat(it.parts()).hasSize(1)
    }

    partListing.parts()[0].also {
      assertThat(it.eTag()).isEqualTo("\"" + hash + "\"")
      assertThat(it.partNumber()).isEqualTo(1)
      assertThat(it.lastModified()).isExactlyInstanceOf(Instant::class.java)
    }
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testListMultipartUploads_ok(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isEmpty()
    val uploadId = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.uploadId()

    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
    }.also { listing ->
      assertThat(listing.uploads()).isNotEmpty
      assertThat(listing.bucket()).isEqualTo(bucketName)
      assertThat(listing.uploads()).hasSize(1)

      listing.uploads()[0]
        .also {
          assertThat(it.uploadId()).isEqualTo(uploadId)
          assertThat(it.key()).isEqualTo(UPLOAD_FILE_NAME)
        }
    }
  }

  /**
   * Tests if empty parts list of not yet completed multipart upload is returned.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testListMultipartUploads_empty(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isEmpty()
    val uploadId = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.uploadId()

    s3ClientV2.listParts {
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

  /**
   * Tests that an exception is thrown when listing parts if the upload id is unknown.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testListMultipartUploads_throwOnUnknownId(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3ClientV2.listParts {
        it.bucket(bucketName)
        it.key("NON_EXISTENT_KEY")
        it.uploadId("NON_EXISTENT_UPLOAD_ID")
      }
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed with prefix filtering.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testListMultipartUploads_withPrefix(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key("key1")
    }
    s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key("key2")
    }

    val listing = s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
      it.prefix("key2")
    }
    assertThat(listing.uploads()).hasSize(1)
    assertThat(listing.uploads()[0].key()).isEqualTo("key2")
  }

  /**
   * Tests if multipart uploads are stored and can be retrieved by bucket.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testListMultipartUploads_multipleBuckets(testInfo: TestInfo) {
    // create multipart upload 1
    val bucketName1 = givenBucket(testInfo)
      .also { name ->
        s3ClientV2.createMultipartUpload {
              it.bucket(name)
              it.key("key1")
          }
      }

    // create multipart upload 2
    val bucketName2 = givenBucket()
      .also { name ->
        s3ClientV2.createMultipartUpload {
          it.bucket(name)
          it.key("key2")
        }
      }

    // assert multipart upload 1
    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName1)
    }.also {
      assertThat(it.uploads()).hasSize(1)
      assertThat(it.uploads()[0].key()).isEqualTo("key1")
    }

    // assert multipart upload 2
    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName2)
    }.also {
      assertThat(it.uploads()).hasSize(1)
      assertThat(it.uploads()[0].key()).isEqualTo("key2")
    }
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testAbortMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads {
          it.bucket(bucketName)
       }.hasUploads()
    ).isFalse

    val uploadId = s3ClientV2
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.uploadId()
    val randomBytes = randomBytes()

    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.hasUploads()
    ).isTrue

    s3ClientV2.listParts {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId)
    }.parts()
      .also {
        assertThat(it).hasSize(1)
        assertThat(it[0].eTag()).isEqualTo(partETag)
      }

    s3ClientV2.abortMultipartUpload {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.uploadId(uploadId)
    }
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.hasUploads()
    ).isFalse

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3ClientV2.listParts {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.uploadId(uploadId)
      }
    }
      .isInstanceOf(AwsServiceException::class.java)
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
  @S3VerifiedSuccess(year = 2024)
  fun testCompleteMultipartUpload_partLeftOut(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = randomName
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isEmpty()

    // Initiate upload
    val uploadId = s3ClientV2
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(key)
      }.uploadId()

    // Upload 3 parts
    val randomBytes1 = randomBytes()
    val partETag1 = uploadPart(bucketName, key, uploadId, 1, randomBytes1)
    val randomBytes2 = randomBytes()
    uploadPart(bucketName, key, uploadId, 2, randomBytes2) //ignore output in this test.
    val randomBytes3 = randomBytes()
    val partETag3 = uploadPart(bucketName, key, uploadId, 3, randomBytes3)

    // Try to complete with these parts
    val result = s3ClientV2.completeMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
      it.multipartUpload {
        it.parts({
            it.eTag(partETag1)
            it.partNumber(1)
          },
          {
            it.eTag(partETag3)
            it.partNumber(3)
          }
      )
      }
    }

    // Verify only 1st and 3rd counts
    (DigestUtils.md5(randomBytes1) + DigestUtils.md5(randomBytes3)).also {
      // verify special etag
      assertThat(result.eTag()).isEqualTo("\"${DigestUtils.md5Hex(it)}-2\"")
    }

    s3ClientV2.getObject {
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
  @S3VerifiedSuccess(year = 2024)
  fun testListParts_completeAndAbort(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = randomName
    assertThat(
      s3ClientV2.listMultipartUploads {
        it.bucket(bucketName)
      }.uploads()
    ).isEmpty()

    // Initiate upload
    val uploadId = s3ClientV2
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(key)
      }.uploadId()

    // Upload part
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, key, uploadId, 1, randomBytes)

    // List parts, make sure we find part 1
    s3ClientV2.listParts {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
    }.parts()
      .also {
        assertThat(it).hasSize(1)
        assertThat(it[0].eTag()).isEqualTo(partETag)
      }

    // Complete
    s3ClientV2.completeMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
      it.multipartUpload {
        it.parts(
          {
            it.eTag(partETag)
            it.partNumber(1)
          }
        )
      }
    }

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3ClientV2.listParts {
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
      }
    }
      .isInstanceOf(AwsServiceException::class.java)
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
  @S3VerifiedSuccess(year = 2024)
  fun shouldCopyPartsAndComplete(testInfo: TestInfo) {
    //Initiate upload
    val bucketName2 = givenBucket()
    val multipartUploadKey = UUID.randomUUID().toString()

    val uploadId = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName2)
      it.key(multipartUploadKey)
    }.uploadId()
    val parts: MutableList<CompletedPart> = ArrayList()

    //bucket for test data
    val bucketName1 = givenBucket(testInfo)

    //create two objects, initiate copy part with full object length
    val sourceKeys = arrayOf(UUID.randomUUID().toString(), UUID.randomUUID().toString())
    val allRandomBytes: MutableList<ByteArray> = ArrayList()
    for (i in sourceKeys.indices) {
      val key = sourceKeys[i]
      val partNumber = i + 1
      val randomBytes = randomBytes()
      val metadata1 = HashMap<String, String>().apply {
        this["contentLength"] = randomBytes.size.toString()
      }
      s3ClientV2.putObject({
          it.bucket(bucketName1)
          it.key(key)
          it.metadata(metadata1)
        },
        RequestBody.fromInputStream(ByteArrayInputStream(randomBytes), randomBytes.size.toLong())
      )

      s3ClientV2.uploadPartCopy {
        it.partNumber(partNumber)
        it.uploadId(uploadId)
        it.destinationBucket(bucketName2)
        it.destinationKey(multipartUploadKey)
        it.sourceKey(key)
        it.sourceBucket(bucketName1)
      }.also {
        val etag = it.copyPartResult().eTag()
        parts.add(CompletedPart.builder().eTag(etag).partNumber(partNumber).build())
        allRandomBytes.add(randomBytes)
      }
    }
    assertThat(allRandomBytes).hasSize(2)

    // Complete with parts
    val result = s3ClientV2.completeMultipartUpload {
      it.bucket(bucketName2)
      it.key(multipartUploadKey)
      it.uploadId(uploadId)
      it.multipartUpload {
        it.parts(parts)
      }
    }
    // Verify parts
    (DigestUtils.md5(allRandomBytes[0]) + DigestUtils.md5(allRandomBytes[1])).also {
      // verify etag
      assertThat(result.eTag()).isEqualTo("\"${DigestUtils.md5Hex(it)}-2\"")
    }

    s3ClientV2.getObject {
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
  @S3VerifiedSuccess(year = 2024)
  fun shouldCopyObjectPart(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val objectMetadata = mapOf(Pair("key", "value"))

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
      it.metadata(objectMetadata)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy {
      it.uploadId(uploadId)
      it.destinationBucket(destinationBucket)
      it.destinationKey(destinationKey)
      it.sourceKey(sourceKey)
      it.sourceBucket(bucketName)
      it.partNumber(1)
      it.copySourceRange("bytes=0-" + (uploadFile.length() - 1))
    }
    val etag = result.copyPartResult().eTag()

    s3ClientV2.listParts {
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
  @S3VerifiedSuccess(year = 2024)
  fun shouldThrowNoSuchKeyOnCopyObjectPartForNonExistingKey(testInfo: TestInfo) {
    val sourceKey = "NON_EXISTENT_KEY"
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val bucketName = givenBucket(testInfo)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
      it.metadata(objectMetadata)
    }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-5")
      }
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchKey")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testUploadPartCopy_successMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy {
      it.uploadId(uploadId)
      it.destinationBucket(destinationBucket)
      it.destinationKey(destinationKey)
      it.sourceKey(sourceKey)
      it.sourceBucket(bucketName)
      it.partNumber(1)
      it.copySourceRange("bytes=0-" + (uploadFile.length() - 1))
      it.copySourceIfMatch(matchingEtag)
    }
    val etag = result.copyPartResult().eTag()

    s3ClientV2.listParts(
      ListPartsRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .build()
    ).also {
      assertThat(it.parts()).hasSize(1)
      assertThat(it.parts()[0].eTag()).isEqualTo(etag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testUploadPartCopy_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
    }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy {
      it.uploadId(uploadId)
      it.destinationBucket(destinationBucket)
      it.destinationKey(destinationKey)
      it.sourceKey(sourceKey)
      it.sourceBucket(bucketName)
      it.partNumber(1)
      it.copySourceRange("bytes=0-" + (uploadFile.length() - 1))
      it.copySourceIfNoneMatch(noneMatchingEtag)
    }
    val etag = result.copyPartResult().eTag()

    s3ClientV2.listParts {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
      it.uploadId(initiateMultipartUploadResult.uploadId())
    }.also {
      assertThat(it.parts()).hasSize(1)
      assertThat(it.parts()[0].eTag()).isEqualTo(etag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testUploadPartCopy_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
    }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-" + uploadFile.length())
        it.copySourceIfMatch(noneMatchingEtag)
      }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testUploadPartCopy_failureNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucket = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult = s3ClientV2.createMultipartUpload {
      it.bucket(destinationBucket)
      it.key(destinationKey)
    }

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy {
        it.uploadId(uploadId)
        it.destinationBucket(destinationBucket)
        it.destinationKey(destinationKey)
        it.sourceKey(sourceKey)
        it.sourceBucket(bucketName)
        it.partNumber(1)
        it.copySourceRange("bytes=0-" + uploadFile.length())
        it.copySourceIfNoneMatch(matchingEtag)
      }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  private fun uploadPart(
    bucketName: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    randomBytes: ByteArray
  ): String {
    return s3ClientV2
      .uploadPart({
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
          it.partNumber(partNumber)
          it.contentLength(randomBytes.size.toLong())
        },
        RequestBody.fromInputStream(ByteArrayInputStream(randomBytes), randomBytes.size.toLong())
      )
      .eTag()
  }
}
