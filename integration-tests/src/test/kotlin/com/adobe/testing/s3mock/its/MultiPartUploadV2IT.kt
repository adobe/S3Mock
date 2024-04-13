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

import com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED
import com.adobe.testing.s3mock.util.DigestUtil.hexDigest
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.ArrayUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.InstanceOfAssertFactories
import org.assertj.core.util.Files
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.springframework.web.util.UriUtils
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest
import software.amazon.awssdk.services.s3.model.ListPartsRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID


internal class MultiPartUploadV2IT : S3TestBase() {
  val s3ClientV2: S3Client = createS3ClientV2()
  val s3AsyncClientV2: S3AsyncClient = createS3AsyncClientV2()
  val s3CrtAsyncClientV2: S3AsyncClient = createS3CrtAsyncClientV2()
  val autoS3CrtAsyncClientV2: S3AsyncClient = createAutoS3CrtAsyncClientV2()
  val transferManagerV2: S3TransferManager = createTransferManagerV2()

  private fun lorem(): String {
    return "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
  }

  @Disabled("This test currently fails. Must debug")
  @Test
  @S3VerifiedTodo
  fun testMultipartUpload_asyncClient(testInfo: TestInfo) {
    //TODO: this could be related - trailing headers for chunks
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = Files.newTemporaryFile()
    java.nio.file.Files.newOutputStream(uploadFile.toPath()).use {
      for(i in 0.. 10000) {
        it.write(lorem().toByteArray())
      }
    }

    s3AsyncClientV2.putObject(
      PutObjectRequest
        .builder()
        .bucket(bucketName)
        .key(uploadFile.name)
        .build(),
      AsyncRequestBody.fromFile(uploadFile)
    ).join()

    s3AsyncClientV2.waiter().waitUntilObjectExists(
      HeadObjectRequest
        .builder()
        .bucket(bucketName)
        .key(uploadFile.name)
        .build()
    )

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(uploadFile.name)
        .build()
    ).use {
      val uploadFileIs = java.nio.file.Files.newInputStream(uploadFile.toPath())
      val uploadDigest = hexDigest(uploadFile)
      val downloadedDigest = hexDigest(it)
      uploadFileIs.close()
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testMultipartUpload_transferManager(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    transferManagerV2
      .uploadFile(
      UploadFileRequest
        .builder()
        .putObjectRequest(
          PutObjectRequest
            .builder()
            .bucket(bucketName)
            .key(UPLOAD_FILE_NAME)
            .build()
        )
        .source(uploadFile)
        .build()
    ).completionFuture().join()

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }

    val downloadFile = Files.newTemporaryFile()
    val downloadFileResult = transferManagerV2.downloadFile(
      DownloadFileRequest
        .builder()
        .getObjectRequest(
          GetObjectRequest
            .builder()
            .bucket(bucketName)
            .key(UPLOAD_FILE_NAME)
            .build()
        )
        .destination(downloadFile)
        .build()
    )

    val completedFileDownload = downloadFileResult.completionFuture().join().response()
    assertThat(completedFileDownload.contentLength()).isEqualTo(uploadFile.length())
    assertThat(downloadFile.length()).isEqualTo(uploadFile.length())
    assertThat(downloadFile).hasSameBinaryContentAs(uploadFile)
  }

  /**
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testMultipartUpload_withUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME)
          .metadata(objectMetadata).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    val uploadPartResult = s3ClientV2.uploadPart(
      UploadPartRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(uploadId)
        .partNumber(1)
        .contentLength(uploadFile.length()).build(),
      //.lastPart(true)
      RequestBody.fromFile(uploadFile),
    )

    s3ClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(
              CompletedPart
                .builder()
                .eTag(uploadPartResult.eTag())
                .partNumber(1)
                .build()
            )
            .build()
        )
        .build()
    )

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .build()
    ).use {
      assertThat(it.response().metadata()).isEqualTo(objectMetadata)
    }
  }

  /**
   * Tests if a multipart upload with the last part being smaller than 5MB works.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME)
          .metadata(objectMetadata).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, >5MB
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    // upload part 2, <5MB
    val uploadPartResult = s3ClientV2.uploadPart(
      UploadPartRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(uploadId)
        .partNumber(2)
        .contentLength(uploadFile.length()).build(),
      //.lastPart(true)
      RequestBody.fromFile(uploadFile),
    )

    val completeMultipartUpload = s3ClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(
              CompletedPart
                .builder()
                .eTag(partETag)
                .partNumber(1)
                .build(),
              CompletedPart
                .builder()
                .eTag(uploadPartResult.eTag())
                .partNumber(2)
                .build()
            )
            .build()
        )
        .build()
    )

    // Verify only 1st and 3rd counts

    val uploadFileBytes = readStreamIntoByteArray(uploadFile.inputStream())
    val allMd5s = ArrayUtils.addAll(
      DigestUtils.md5(randomBytes),
      *DigestUtils.md5(uploadFileBytes)
    )

    // verify special etag
    assertThat(completeMultipartUpload.eTag()).isEqualTo("\"" + DigestUtils.md5Hex(allMd5s) + "-2" + "\"")

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      // verify content size
      assertThat(it.response().contentLength()).isEqualTo(randomBytes.size.toLong() + uploadFileBytes.size.toLong())
      // verify contents
      assertThat(readStreamIntoByteArray(it.buffered())).isEqualTo(concatByteArrays(randomBytes, uploadFileBytes))
    }

    assertThat(completeMultipartUpload.location())
      .isEqualTo("${serviceEndpoint}/$bucketName/${UriUtils.encode(UPLOAD_FILE_NAME, StandardCharsets.UTF_8)}")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testInitiateMultipartAndRetrieveParts(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val hash = DigestUtils.md5Hex(FileInputStream(uploadFile))
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME)
          .metadata(objectMetadata).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    val key = initiateMultipartUploadResult.key()

    s3ClientV2.uploadPart(
      UploadPartRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(key)
        .uploadId(uploadId)
        .partNumber(1)
        .contentLength(uploadFile.length()).build(),
      //.lastPart(true)
      RequestBody.fromFile(uploadFile),
    )

    val listPartsRequest = ListPartsRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .uploadId(uploadId)
      .build()
    val partListing = s3ClientV2.listParts(listPartsRequest)
    assertThat(partListing.parts()).hasSize(1)

    val partSummary = partListing.parts()[0]
    assertThat(partSummary.eTag()).isEqualTo("\"" + hash + "\"")
    assertThat(partSummary.partNumber()).isEqualTo(1)
    assertThat(partSummary.lastModified()).isExactlyInstanceOf(Instant::class.java)
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testListMultipartUploads_ok(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      )
        .uploads()
    ).isEmpty()
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    val listing = s3ClientV2.listMultipartUploads(
      ListMultipartUploadsRequest.builder().bucket(bucketName).build()
    )
    assertThat(listing.uploads()).isNotEmpty
    assertThat(listing.bucket()).isEqualTo(bucketName)
    assertThat(listing.uploads()).hasSize(1)

    val upload = listing.uploads()[0]
    assertThat(upload.uploadId()).isEqualTo(uploadId)
    assertThat(upload.key()).isEqualTo(UPLOAD_FILE_NAME)
  }

  /**
   * Tests if empty parts list of not yet completed multipart upload is returned.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testListMultipartUploads_empty(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      ).uploads()
    ).isEmpty()
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    val listing = s3ClientV2
      .listParts(
        ListPartsRequest
          .builder()
          .bucket(bucketName)
          .key(UPLOAD_FILE_NAME)
          .uploadId(uploadId)
          .build()
      )
    assertThat(listing.parts()).isEmpty()
    assertThat(listing.bucket()).isEqualTo(bucketName)
    assertThat(listing.uploadId()).isEqualTo(uploadId)
    assertThat(SdkHttpUtils.urlDecode(listing.key())).isEqualTo(UPLOAD_FILE_NAME)
  }

  /**
   * Tests that an exception is thrown when listing parts if the upload id is unknown.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testListMultipartUploads_throwOnUnknownId(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    assertThatThrownBy {
      s3ClientV2.listParts(
        ListPartsRequest.builder().bucket(bucketName).key("NON_EXISTENT_KEY").uploadId(
          "NON_EXISTENT_UPLOAD_ID"
        ).build()
      )
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed with prefix filtering.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testListMultipartUploads_withPrefix(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key("key1").build()
      )
    s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key("key2").build()
      )
    val listMultipartUploadsRequest = ListMultipartUploadsRequest.builder().bucket(bucketName).prefix("key2").build()

    val listing = s3ClientV2.listMultipartUploads(listMultipartUploadsRequest)
    assertThat(listing.uploads()).hasSize(1)
    assertThat(listing.uploads()[0].key()).isEqualTo("key2")
  }

  /**
   * Tests if multipart uploads are stored and can be retrieved by bucket.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testListMultipartUploads_multipleBuckets(testInfo: TestInfo) {
    // create multipart upload 1
    val bucketName1 = givenBucketV2(testInfo)
    s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName1).key("key1").build()
      )
    // create multipart upload 2
    val bucketName2 = givenRandomBucketV1()
    s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName2).key("key2").build()
      )

    // assert multipart upload 1
    val listMultipartUploadsRequest1 = ListMultipartUploadsRequest.builder().bucket(bucketName1).build()
    val listing = s3ClientV2.listMultipartUploads(listMultipartUploadsRequest1)
    assertThat(listing.uploads()).hasSize(1)
    assertThat(listing.uploads()[0].key()).isEqualTo("key1")

    // assert multipart upload 2
    val listMultipartUploadsRequest2 = ListMultipartUploadsRequest.builder().bucket(bucketName2).build()
    val listing2 = s3ClientV2.listMultipartUploads(listMultipartUploadsRequest2)
    assertThat(listing2.uploads()).hasSize(1)
    assertThat(listing2.uploads()[0].key()).isEqualTo("key2")
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testAbortMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      ).hasUploads()
    ).isFalse

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    val randomBytes = randomBytes()

    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      ).hasUploads()
    ).isTrue

    val partsBeforeComplete =
      s3ClientV2.listParts(
        ListPartsRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME).uploadId(uploadId)
          .build()
      ).parts()
    assertThat(partsBeforeComplete).hasSize(1)
    assertThat(partsBeforeComplete[0].eTag()).isEqualTo(partETag)

    s3ClientV2.abortMultipartUpload(
      AbortMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME)
        .uploadId(uploadId).build()
    )
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      ).hasUploads()
    ).isFalse

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3ClientV2.listParts(
        ListPartsRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME).uploadId(
          uploadId
        ).build()
      )
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
  @S3VerifiedSuccess(year = 2022)
  fun testCompleteMultipartUpload_partLeftOut(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = randomName
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      ).uploads()
    ).isEmpty()

    // Initiate upload
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    // Upload 3 parts
    val randomBytes1 = randomBytes()
    val partETag1 = uploadPart(bucketName, key, uploadId, 1, randomBytes1)
    val randomBytes2 = randomBytes()
    uploadPart(bucketName, key, uploadId, 2, randomBytes2) //ignore output in this test.
    val randomBytes3 = randomBytes()
    val partETag3 = uploadPart(bucketName, key, uploadId, 3, randomBytes3)

    // Adding to parts list only 1st and 3rd part
    val parts: MutableList<String> = ArrayList()
    parts.add(partETag1)
    parts.add(partETag3)

    // Try to complete with these parts
    val result = s3ClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest.builder()
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(
              CompletedPart
                .builder()
                .eTag(partETag1)
                .partNumber(1)
                .build(),
              CompletedPart
                .builder()
                .eTag(partETag3)
                .partNumber(3)
                .build()
            )
            .build()
        )
        .build()
    )

    // Verify only 1st and 3rd counts

    val allMd5s = ArrayUtils.addAll(
      DigestUtils.md5(randomBytes1),
      *DigestUtils.md5(randomBytes3)
    )

    // verify special etag
    assertThat(result.eTag()).isEqualTo("\"" + DigestUtils.md5Hex(allMd5s) + "-2" + "\"")

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
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
  @S3VerifiedSuccess(year = 2022)
  fun testListParts_completeAndAbort(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = randomName
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest.builder().bucket(bucketName).build()
      )
        .uploads()
    ).isEmpty()

    // Initiate upload
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    // Upload part
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, key, uploadId, 1, randomBytes)

    // List parts, make sure we find part 1
    val partsBeforeComplete = s3ClientV2.listParts(
      ListPartsRequest
        .builder()
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .build()
    ).parts()
    assertThat(partsBeforeComplete).hasSize(1)
    assertThat(partsBeforeComplete[0].eTag()).isEqualTo(partETag)

    // Complete
    s3ClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(
              CompletedPart
                .builder()
                .eTag(partETag)
                .partNumber(1)
                .build()
            )
            .build()
        )
        .build()
    )

    // List parts, make sure we find no parts
    assertThatThrownBy {
      s3ClientV2.listParts(
        ListPartsRequest
          .builder()
          .bucket(bucketName)
          .key(key)
          .uploadId(uploadId)
          .build()
      )
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyPartsAndComplete(testInfo: TestInfo) {
    //Initiate upload
    val bucketName2 = givenRandomBucketV2()
    val multipartUploadKey = UUID.randomUUID().toString()

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName2).key(multipartUploadKey).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    val parts: MutableList<CompletedPart> = ArrayList()

    //bucket for test data
    val bucketName1 = givenBucketV2(testInfo)

    //create two objects, initiate copy part with full object length
    val sourceKeys = arrayOf(UUID.randomUUID().toString(), UUID.randomUUID().toString())
    val allRandomBytes: MutableList<ByteArray> = ArrayList()
    for (i in sourceKeys.indices) {
      val key = sourceKeys[i]
      val partNumber = i + 1
      val randomBytes = randomBytes()
      val metadata1 = HashMap<String, String>()
      metadata1["contentLength"] = randomBytes.size.toString()
      s3ClientV2.putObject(
        PutObjectRequest
          .builder()
          .bucket(bucketName1)
          .key(key)
          .metadata(metadata1)
          .build(),
        RequestBody.fromInputStream(ByteArrayInputStream(randomBytes), randomBytes.size.toLong())
      )

      val result = s3ClientV2.uploadPartCopy(
        UploadPartCopyRequest.builder()
          .partNumber(partNumber)
          .uploadId(uploadId)
          .destinationBucket(bucketName2)
          .destinationKey(multipartUploadKey)
          .sourceKey(key)
          .sourceBucket(bucketName1).build()
      )
      val etag = result.copyPartResult().eTag()
      parts.add(CompletedPart.builder().eTag(etag).partNumber(partNumber).build())
      allRandomBytes.add(randomBytes)
    }
    assertThat(allRandomBytes).hasSize(2)

    // Complete with parts
    val result = s3ClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(bucketName2)
        .key(multipartUploadKey)
        .uploadId(uploadId)
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(parts)
            .build()
        )
        .build()
    )

    // Verify parts
    val allMd5s = ArrayUtils.addAll(
      DigestUtils.md5(allRandomBytes[0]),
      *DigestUtils.md5(allRandomBytes[1])
    )

    // verify etag
    assertThat(result.eTag()).isEqualTo("\"" + DigestUtils.md5Hex(allMd5s) + "-2" + "\"")

    s3ClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName2)
        .key(multipartUploadKey)
        .build()
    ).use {
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectPart(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val objectMetadata = mapOf(Pair("key", "value"))

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey)
          .metadata(objectMetadata).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy(
      UploadPartCopyRequest.builder()
        .uploadId(uploadId)
        .destinationBucket(destinationBucket)
        .destinationKey(destinationKey)
        .sourceKey(sourceKey)
        .sourceBucket(bucketName)
        .partNumber(1)
        .copySourceRange("bytes=0-" + (uploadFile.length() -1))
        .build()
    )
    val etag = result.copyPartResult().eTag()

    val partListing = s3ClientV2.listParts(
      ListPartsRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .build()
    )
    assertThat(partListing.parts()).hasSize(1)
    assertThat(partListing.parts()[0].eTag()).isEqualTo(etag)
  }

  /**
   * Tries to copy part of a non-existing object to a new bucket.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldThrowNoSuchKeyOnCopyObjectPartForNonExistingKey(testInfo: TestInfo) {
    val sourceKey = "NON_EXISTENT_KEY"
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val bucketName = givenBucketV2(testInfo)
    val objectMetadata = mapOf(Pair("key", "value"))
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey)
          .metadata(objectMetadata).build()
      )

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy(
        UploadPartCopyRequest.builder()
          .uploadId(uploadId)
          .destinationBucket(destinationBucket)
          .destinationKey(destinationKey)
          .sourceKey(sourceKey)
          .sourceBucket(bucketName)
          .partNumber(1)
          .copySourceRange("bytes=0-5")
          .build()
      )
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
    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy(
      UploadPartCopyRequest.builder()
        .uploadId(uploadId)
        .destinationBucket(destinationBucket)
        .destinationKey(destinationKey)
        .sourceKey(sourceKey)
        .sourceBucket(bucketName)
        .partNumber(1)
        .copySourceRange("bytes=0-" + (uploadFile.length() -1))
        .copySourceIfMatch(matchingEtag)
        .build()
    )
    val etag = result.copyPartResult().eTag()

    val partListing = s3ClientV2.listParts(
      ListPartsRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .build()
    )
    assertThat(partListing.parts()).hasSize(1)
    assertThat(partListing.parts()[0].eTag()).isEqualTo(etag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testUploadPartCopy_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey).build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()

    val result = s3ClientV2.uploadPartCopy(
      UploadPartCopyRequest.builder()
        .uploadId(uploadId)
        .destinationBucket(destinationBucket)
        .destinationKey(destinationKey)
        .sourceKey(sourceKey)
        .sourceBucket(bucketName)
        .partNumber(1)
        .copySourceRange("bytes=0-" + (uploadFile.length() -1))
        .copySourceIfNoneMatch(noneMatchingEtag)
        .build()
    )
    val etag = result.copyPartResult().eTag()

    val partListing = s3ClientV2.listParts(
      ListPartsRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .build()
    )
    assertThat(partListing.parts()).hasSize(1)
    assertThat(partListing.parts()[0].eTag()).isEqualTo(etag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testUploadPartCopy_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey).build()
      )

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy(
        UploadPartCopyRequest.builder()
          .uploadId(uploadId)
          .destinationBucket(destinationBucket)
          .destinationKey(destinationKey)
          .sourceKey(sourceKey)
          .sourceBucket(bucketName)
          .partNumber(1)
          .copySourceRange("bytes=0-" + uploadFile.length())
          .copySourceIfMatch(noneMatchingEtag)
          .build()
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testUploadPartCopy_failureNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(sourceKey)
    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucket = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResponse.eTag()

    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(destinationBucket).key(destinationKey).build()
      )

    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThatThrownBy {
      s3ClientV2.uploadPartCopy(
        UploadPartCopyRequest.builder()
          .uploadId(uploadId)
          .destinationBucket(destinationBucket)
          .destinationKey(destinationKey)
          .sourceKey(sourceKey)
          .sourceBucket(bucketName)
          .partNumber(1)
          .copySourceRange("bytes=0-" + uploadFile.length())
          .copySourceIfNoneMatch(matchingEtag)
          .build()
      )
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
      .uploadPart(
        UploadPartRequest.builder()
          .bucket(bucketName)
          .key(key)
          .uploadId(uploadId)
          .partNumber(partNumber)
          .contentLength(randomBytes.size.toLong()).build(),
        RequestBody.fromInputStream(ByteArrayInputStream(randomBytes), randomBytes.size.toLong())
      )
      .eTag()
  }
}
