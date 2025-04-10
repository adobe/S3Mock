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

import com.adobe.testing.s3mock.util.DigestUtil.hexDigest
import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.Headers
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model.DeleteObjectsResult
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.GetObjectMetadataRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.ResponseHeaderOverrides
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import com.amazonaws.services.s3.transfer.TransferManager
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.configuration.Configuration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.UUID
import java.util.stream.Collectors
import kotlin.math.min

/**
 * Test the application using the AmazonS3 SDK V1.
 */
@Deprecated("* AWS has deprecated SDK for Java v1, and will remove support EOY 2025.\n" +
  "    * S3Mock will remove usage of Java v1 early 2026.")
internal class GetPutDeleteObjectV1IT : S3TestBase() {

  private val httpClient: CloseableHttpClient = createHttpClient()
  private val s3Client: AmazonS3 = createS3ClientV1()
  private val transferManagerV1: TransferManager = createTransferManagerV1()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun putObjectWhereKeyContainsPathFragments(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val objectExist = s3Client.doesObjectExist(bucketName, UPLOAD_FILE_NAME)
    assertThat(objectExist).isTrue
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   */
  @ParameterizedTest(name = ParameterizedTest.INDEX_PLACEHOLDER + " uploadWithSigning={0}, uploadChunked={1}")
  @CsvSource(value = ["true, true", "true, false", "false, true", "false, false"])
  @S3VerifiedSuccess(year = 2024)
  fun shouldUploadAndDownloadObject(uploadWithSigning: Boolean, uploadChunked: Boolean,
                                    testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadClient = defaultTestAmazonS3ClientBuilder()
      .withPayloadSigningEnabled(uploadWithSigning)
      .withChunkedEncodingDisabled(uploadChunked)
      .build()
    uploadClient.putObject(PutObjectRequest(bucketName, uploadFile.name, uploadFile))
    s3Client.getObject(bucketName, uploadFile.name).also {
      assertThat(it.objectMetadata.contentLength).isEqualTo(uploadFile.length())
      verifyObjectContent(uploadFile, it)
    }
  }

  /**
   * Uses weird, but valid characters in the key used to store an object.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldTolerateWeirdCharactersInObjectKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val weirdStuff = "$&_ .,':\u0001" // use only characters that are safe or need special handling
    val key = weirdStuff + uploadFile.name + weirdStuff
    s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))

    s3Client.getObject(bucketName, key).also {
      verifyObjectContent(uploadFile, it)
    }
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldUploadAndDownloadStream(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val resourceId = UUID.randomUUID().toString()
    val contentEncoding = "gzip"
    val resource = byteArrayOf(1, 2, 3, 4, 5)
    val inputStream = ByteArrayInputStream(resource)
    val objectMetadata = ObjectMetadata().apply {
      this.contentLength = resource.size.toLong()
      this.contentEncoding = contentEncoding
    }
    val putObjectRequest = PutObjectRequest(bucketName, resourceId, inputStream, objectMetadata)
    transferManagerV1.upload(putObjectRequest).also {
      it.waitForUploadResult()
    }
    s3Client.getObject(bucketName, resourceId).use {
      assertThat(it.objectMetadata.contentEncoding).isEqualTo(contentEncoding)
      val uploadDigest = hexDigest(ByteArrayInputStream(resource))
      val downloadedDigest = hexDigest(it.objectContent)
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  @S3VerifiedFailure(year = 2024,
    reason = "No KMS configuration for AWS test account")
  fun shouldUploadWithEncryption(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectKey = UPLOAD_FILE_NAME
    val metadata = ObjectMetadata().apply {
      this.addUserMetadata("key", "value")
    }
    val putObjectRequest = PutObjectRequest(bucketName, objectKey, uploadFile)
      .withMetadata(metadata)
      .apply {
        this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
      }
    s3Client.putObject(putObjectRequest)
    val getObjectMetadataRequest = GetObjectMetadataRequest(bucketName, objectKey)
    s3Client.getObjectMetadata(getObjectMetadataRequest).also {
      assertThat(it.contentLength).isEqualTo(uploadFile.length())
      assertThat(it.userMetadata).isEqualTo(metadata.userMetadata)
      assertThat(it.sseAwsKmsKeyId).isEqualTo(TEST_ENC_KEY_ID)
    }
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldNotUploadWithWrongEncryptionKey(testInfo: TestInfo) {
    Configuration().apply {
      this.setMaxStackTraceElementsDisplayed(10000)
      this.apply()
    }
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy { s3Client
      .putObject(
        PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile)
          .apply {
            this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
          }
      ) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldNotUploadStreamingWithWrongEncryptionKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val bytes = UPLOAD_FILE_NAME.toByteArray()
    val stream: InputStream = ByteArrayInputStream(bytes)
    val objectKey = UUID.randomUUID().toString()
    assertThatThrownBy { s3Client
      .putObject(
        PutObjectRequest(bucketName, objectKey, stream,
          ObjectMetadata().apply {
            this.contentLength = bytes.size.toLong()
          }
        ).apply {
          this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
        }
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldGetObjectMetadata(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val nonExistingFileName = randomName
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = ObjectMetadata().apply {
      this.addUserMetadata("key", "value")
      this.contentEncoding = "gzip"
    }
    val putObjectResult = s3Client.putObject(
      PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile).apply {
        this.withMetadata(objectMetadata)
      }
    )
    s3Client.getObjectMetadata(bucketName, UPLOAD_FILE_NAME).also {
      assertThat(it.contentEncoding).isEqualTo("gzip")
      assertThat(it.eTag).isEqualTo(putObjectResult.eTag)
      assertThat(it.userMetadata).isEqualTo(objectMetadata.userMetadata)
    }
    assertThatThrownBy {
      s3Client.getObjectMetadata(
        bucketName,
        nonExistingFileName
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404")
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldDeleteObject(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    s3Client.deleteObject(bucketName, UPLOAD_FILE_NAME)
    assertThatThrownBy { s3Client.getObjectMetadata(bucketName, UPLOAD_FILE_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404")
  }

  /**
   * Tests if multiple objects can be deleted.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldBatchDeleteObjects(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile1 = File(UPLOAD_FILE_NAME)
    val uploadFile2 = File(UPLOAD_FILE_NAME)
    val uploadFile3 = File(UPLOAD_FILE_NAME)
    val file1 = "1_$UPLOAD_FILE_NAME"
    val file2 = "2_$UPLOAD_FILE_NAME"
    val file3 = "3_$UPLOAD_FILE_NAME"
    s3Client.putObject(PutObjectRequest(bucketName, file1, uploadFile1))
    s3Client.putObject(PutObjectRequest(bucketName, file2, uploadFile2))
    s3Client.putObject(PutObjectRequest(bucketName, file3, uploadFile3))
    val delObjRes = s3Client.deleteObjects(
      DeleteObjectsRequest(bucketName).apply {
        this.keys = ArrayList<KeyVersion>().apply {
          this.add(KeyVersion(file1))
          this.add(KeyVersion(file2))
          this.add(KeyVersion(file3))
        }
      }
    )
    assertThat(delObjRes.deletedObjects.size).isEqualTo(3)
    assertThat(
      delObjRes.deletedObjects.stream()
        .map { obj: DeleteObjectsResult.DeletedObject -> obj.key }
        .collect(Collectors.toList()))
      .contains(file1, file2, file3)
    assertThatThrownBy { s3Client.getObjectMetadata(bucketName, UPLOAD_FILE_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404")
  }

  /**
   * Tests if Error is thrown when DeleteObjectsRequest contains nonExisting key.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldThrowOnBatchDeleteObjectsWrongKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile1 = File(UPLOAD_FILE_NAME)
    val file1 = "1_$UPLOAD_FILE_NAME"
    val nonExistingFile = "4_" + UUID.randomUUID()
    s3Client.putObject(PutObjectRequest(bucketName, file1, uploadFile1))
    val multiObjectDeleteRequest = DeleteObjectsRequest(bucketName).apply {
      this.keys = ArrayList<KeyVersion>().apply {
        this.add(KeyVersion(file1))
        this.add(KeyVersion(nonExistingFile))
      }
    }
    val delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest)
    assertThat(delObjRes.deletedObjects.size).isEqualTo(2)
    assertThat(
      delObjRes.deletedObjects.stream()
        .map { obj: DeleteObjectsResult.DeletedObject -> obj.key }
        .collect(Collectors.toList()))
      .contains(file1, nonExistingFile)
  }

  /**
   * Tests if an object can be uploaded asynchronously.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldUploadInParallel(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    transferManagerV1.upload(PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile)).also { upload ->
      upload.waitForUploadResult().also {
        assertThat(it.key).isEqualTo(UPLOAD_FILE_NAME)
      }
    }
    s3Client.getObject(bucketName, UPLOAD_FILE_NAME).also {
      assertThat(it.key).isEqualTo(UPLOAD_FILE_NAME)
    }
  }

  /**
   * Verify that range-downloads work.
   */
  @Test
  @S3VerifiedSuccess(year = 2024)
  fun checkRangeDownloads(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val upload = transferManagerV1.upload(PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile))
    upload.waitForUploadResult()

    val smallRequestStartBytes = 1L
    val smallRequestEndBytes = 2L
    val downloadFile1 = File.createTempFile(UUID.randomUUID().toString(), null)
    transferManagerV1.download(
      GetObjectRequest(bucketName, UPLOAD_FILE_NAME)
        .withRange(smallRequestStartBytes, smallRequestEndBytes), downloadFile1
    ).also { download ->
      download.waitForCompletion()
      assertThat(downloadFile1.length()).isEqualTo(smallRequestEndBytes)
      assertThat(download.objectMetadata.instanceLength).isEqualTo(uploadFile.length())
      assertThat(download.objectMetadata.contentLength).isEqualTo(smallRequestEndBytes)
    }

    val largeRequestStartBytes = 0L
    val largeRequestEndBytes = 1000L
    val downloadFile2 = File.createTempFile(UUID.randomUUID().toString(), null)
    transferManagerV1
      .download(
        GetObjectRequest(bucketName, UPLOAD_FILE_NAME).withRange(largeRequestStartBytes, largeRequestEndBytes),
        downloadFile2
      ).also { download ->
        download.waitForCompletion()
        assertThat(downloadFile2.length()).isEqualTo(min(uploadFile.length(), largeRequestEndBytes + 1))
        assertThat(download.objectMetadata.instanceLength).isEqualTo(uploadFile.length())
        assertThat(download.objectMetadata.contentLength).isEqualTo(min(uploadFile.length(), largeRequestEndBytes + 1))
        assertThat(download.objectMetadata.contentRange)
          .containsExactlyElementsOf(listOf(largeRequestStartBytes, min(uploadFile.length() - 1, largeRequestEndBytes)))
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = hexDigest(uploadFileIs)
    assertThat(putObjectResult.eTag).isEqualTo(expectedEtag)

    s3Client.getObject(GetObjectRequest(bucketName, UPLOAD_FILE_NAME)
      .withMatchingETagConstraint("\"${putObjectResult.eTag}\"")).also {
      //v1 SDK does not return ETag on GetObject. Can only check if response is returned here.
      assertThat(it.objectContent).isNotNull
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetObject_failureWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = hexDigest(uploadFileIs)
    assertThat(putObjectResult.eTag).isEqualTo(expectedEtag)

    val nonMatchingEtag = "\"$randomName\""
    s3Client.getObject(GetObjectRequest(bucketName, UPLOAD_FILE_NAME)
      .withMatchingETagConstraint(nonMatchingEtag)).also {
      //v1 SDK does not return a 412 error on a non-matching GetObject. Check if response is null.
      assertThat(it).isNull()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetObject_successWithNonMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = hexDigest(uploadFileIs)
    assertThat(putObjectResult.eTag).isEqualTo(expectedEtag)

    val nonMatchingEtag = "\"$randomName\""
    s3Client.getObject(GetObjectRequest(bucketName, UPLOAD_FILE_NAME)
      .withNonmatchingETagConstraint(nonMatchingEtag)).also {
      //v1 SDK does not return ETag on GetObject. Can only check if response is returned here.
      assertThat(it.objectContent).isNotNull
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testGetObject_failureWithNonMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = hexDigest(uploadFileIs)
    assertThat(putObjectResult.eTag).isEqualTo(expectedEtag)

    s3Client.getObject(
      GetObjectRequest(bucketName, UPLOAD_FILE_NAME)
        .withNonmatchingETagConstraint("\"${putObjectResult.eTag}\"")
    ).also {
      //v1 SDK does not return a 412 error on a non-matching GetObject. Check if response is null.
      assertThat(it).isNull()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun generatePresignedUrlWithResponseHeaderOverrides(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val presignedUrlRequest = GeneratePresignedUrlRequest(bucketName, UPLOAD_FILE_NAME).apply {
      this.withResponseHeaders(
        ResponseHeaderOverrides().apply {
          this.cacheControl = "cacheControl"
          this.contentDisposition = "contentDisposition"
          this.contentEncoding = "contentEncoding"
          this.contentLanguage = "contentLanguage"
          this.contentType = "my/contentType"
          this.expires = "expires"
        }
      )
      this.method = HttpMethod.GET
    }
    val resourceUrl = s3Client.generatePresignedUrl(presignedUrlRequest)
    httpClient.use {
      val getObject = HttpGet(resourceUrl.toString())
      it.execute(getObject).also { response ->
        assertThat(response.getFirstHeader(Headers.CACHE_CONTROL).value).isEqualTo("cacheControl")
        assertThat(response.getFirstHeader(Headers.CONTENT_DISPOSITION).value).isEqualTo("contentDisposition")
        assertThat(response.getFirstHeader(Headers.CONTENT_ENCODING).value).isEqualTo("contentEncoding")
        assertThat(response.getFirstHeader(Headers.CONTENT_LANGUAGE).value).isEqualTo("contentLanguage")
        assertThat(response.getFirstHeader(Headers.CONTENT_TYPE).value).isEqualTo("my/contentType")
        assertThat(response.getFirstHeader(Headers.EXPIRES).value).isEqualTo("expires")
      }
    }
  }
}
