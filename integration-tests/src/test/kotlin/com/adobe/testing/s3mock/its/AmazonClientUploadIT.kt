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

import com.adobe.testing.s3mock.util.DigestUtil
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.Headers
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.Bucket
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model.DeleteObjectsResult
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.GetObjectMetadataRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.GetObjectTaggingRequest
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.MetadataDirective
import com.amazonaws.services.s3.model.MultiObjectDeleteException
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.ObjectTagging
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.ResponseHeaderOverrides
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import com.amazonaws.services.s3.model.SetObjectTaggingRequest
import com.amazonaws.services.s3.model.Tag
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.net.URL
import java.net.URLConnection
import java.security.KeyManagementException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.Date
import java.util.UUID
import java.util.stream.Collectors
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSession
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/**
 * Test the application using the AmazonS3 client.
 */
internal class AmazonClientUploadIT : S3TestBase() {
  /**
   * Verify that buckets can be created and listed.
   */
  @Test
  fun shouldCreateBucketAndListAllBuckets() {
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    val creationDate = Date(System.currentTimeMillis() / 1000 * 1000 - 60000)
    val bucket = s3Client!!.createBucket(BUCKET_NAME)
    assertThat(bucket.name)
      .`as`(String.format("Bucket name should match '%s'!", BUCKET_NAME))
      .isEqualTo(BUCKET_NAME)
    val buckets = s3Client!!.listBuckets().stream().filter { b: Bucket -> BUCKET_NAME == b.name }
      .collect(Collectors.toList())
    assertThat(buckets).`as`("Expecting one bucket").hasSize(1)
    val createdBucket = buckets[0]
    assertThat(createdBucket.creationDate).isAfterOrEqualTo(creationDate)
    val bucketOwner = createdBucket.owner
    assertThat(bucketOwner.displayName).isEqualTo("s3-mock-file-store")
    assertThat(bucketOwner.id).isEqualTo("123")
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  fun defaultBucketsGotCreated() {
    val buckets = s3Client!!.listBuckets()
    val bucketNames = buckets.stream()
      .map { obj: Bucket -> obj.name }
      .filter { o: String? -> INITIAL_BUCKET_NAMES.contains(o) }
      .collect(Collectors.toSet())
    assertThat(bucketNames)
      .containsAll(INITIAL_BUCKET_NAMES)
      .`as`("Not all default Buckets got created")
  }

  /**
   * Verifies [AmazonS3.doesObjectExist].
   */
  @Test
  fun putObjectWhereKeyContainsPathFragments() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    val objectExist = s3Client!!.doesObjectExist(BUCKET_NAME, UPLOAD_FILE_NAME)
    assertThat(objectExist).isTrue
  }

  /**
   * Stores files in a previously created bucket. List files using ListObjectsV2Request
   */
  @Test
  fun shouldUploadAndListV2Objects() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(
      PutObjectRequest(
        BUCKET_NAME,
        uploadFile.name, uploadFile
      )
    )
    s3Client!!.putObject(
      PutObjectRequest(
        BUCKET_NAME,
        uploadFile.name + "copy1", uploadFile
      )
    )
    s3Client!!.putObject(
      PutObjectRequest(
        BUCKET_NAME,
        uploadFile.name + "copy2", uploadFile
      )
    )
    val listReq = ListObjectsV2Request()
      .withBucketName(BUCKET_NAME)
      .withMaxKeys(3)
    val listResult = s3Client!!.listObjectsV2(listReq)
    assertThat(listResult.keyCount).isEqualTo(3)
    for (objectSummary in listResult.objectSummaries) {
      assertThat(objectSummary.key).contains(uploadFile.name)
      val s3Object = s3Client!!.getObject(BUCKET_NAME, objectSummary.key)
      verifyObjectContent(uploadFile, s3Object)
    }
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   */
  @ParameterizedTest(name = ParameterizedTest.INDEX_PLACEHOLDER + " uploadWithSigning={0}, uploadChunked={1}")
  @CsvSource(value = ["true, true", "true, false", "false, true", "false, false"])
  fun shouldUploadAndDownloadObject(uploadWithSigning: Boolean, uploadChunked: Boolean) {
    s3Client!!.createBucket(BUCKET_NAME)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadClient = defaultTestAmazonS3ClientBuilder()
      .withPayloadSigningEnabled(uploadWithSigning)
      .withChunkedEncodingDisabled(uploadChunked)
      .build()
    uploadClient.putObject(PutObjectRequest(BUCKET_NAME, uploadFile.name, uploadFile))
    val s3Object = s3Client!!.getObject(BUCKET_NAME, uploadFile.name)
    verifyObjectContent(uploadFile, s3Object)
  }

  /**
   * Uses weird, but valid characters in the key used to store an object.
   */
  @Test
  fun shouldTolerateWeirdCharactersInObjectKey() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val weirdStuff = ("\\$%&_+.,~|\"':^"
      + "\u1234\uabcd\u0001") // non-ascii and unprintable stuff
    val key = weirdStuff + uploadFile.name + weirdStuff
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, key, uploadFile))
    val s3Object = s3Client!!.getObject(BUCKET_NAME, key)
    verifyObjectContent(uploadFile, s3Object)
  }

  private fun verifyObjectContent(uploadFile: File, s3Object: S3Object) {
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val uploadDigest = DigestUtil.getHexDigest(uploadFileIs)
    val downloadedDigest = DigestUtil.getHexDigest(s3Object.objectContent)
    uploadFileIs.close()
    s3Object.close()
    assertThat(uploadDigest)
      .isEqualTo(downloadedDigest)
      .`as`("Up- and downloaded Files should have equal digests")
  }

  /**
   * Uses weird, but valid characters in the key used to store an object. Verifies
   * that ListObject returns the correct object names.
   */
  @Test
  fun shouldListWithCorrectObjectNames() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val weirdStuff = ("\\$%&_ .,~|\"':^"
      + "\u1234\uabcd\u0001") // non-ascii and unprintable stuff
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = prefix + weirdStuff + uploadFile.name + weirdStuff
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, key, uploadFile))
    val listing = s3Client!!.listObjects(BUCKET_NAME, prefix)
    val summaries = listing.objectSummaries
    assertThat(summaries)
      .`as`("Must have exactly one match")
      .hasSize(1)
    assertThat(summaries[0].key)
      .`as`("Object name must match")
      .isEqualTo(key)
  }

  /**
   * Same as [shouldListWithCorrectObjectNames] but for V2 API.
   */
  @Test
  fun shouldListV2WithCorrectObjectNames() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val weirdStuff = ("\\$%&_ .,~|\"':^"
      + "\u1234\uabcd\u0001") // non-ascii and unprintable stuff
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = prefix + weirdStuff + uploadFile.name + weirdStuff
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, key, uploadFile))

    // AWS client ListObjects V2 defaults to no encoding whereas V1 defaults to URL
    val lorv2 = ListObjectsV2Request()
    lorv2.bucketName = BUCKET_NAME
    lorv2.prefix = prefix
    lorv2.encodingType = "url" // do use encoding!
    val listing = s3Client!!.listObjectsV2(lorv2)
    val summaries = listing.objectSummaries
    assertThat(summaries)
      .`as`("Must have exactly one match")
      .hasSize(1)
    assertThat(summaries[0].key)
      .`as`("Object name must match")
      .isEqualTo(key)
  }

  /**
   * Uses a key that cannot be represented in XML without encoding. Then lists
   * the objects without encoding, expecting a parse exception and thus verifying
   * that the encoding parameter is honored.
   *
   *
   * This isn't the greatest way to test this functionality, however, there
   * is currently no low-level testing infrastructure in place.
   */
  @Test
  fun shouldHonorEncodingType() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val prefix = "shouldHonorEncodingType/"
    val key = prefix + "\u0001" // key invalid in XML
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, key, uploadFile))
    val lor = ListObjectsRequest(BUCKET_NAME, prefix, null, null, null)
    lor.encodingType = "" // don't use encoding

    //Starting in Spring Boot 2.6, Jackson is not able to encode the key properly if it's not
    // encoded by S3Mock. S3ObjectSummary will have empty key in this case.
    val listing = s3Client!!.listObjects(lor)
    val summaries = listing.objectSummaries
    assertThat(summaries)
      .`as`("Must have exactly one match")
      .hasSize(1)
    assertThat(summaries[0].key)
      .`as`("Object name must match")
      .isEqualTo("")
  }

  /**
   * The same as [shouldHonorEncodingType] but for V2 API.
   */
  @Test
  fun shouldHonorEncodingTypeV2() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val prefix = "shouldHonorEncodingType/"
    val key = prefix + "\u0001" // key invalid in XML
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, key, uploadFile))
    val lorv2 = ListObjectsV2Request()
    lorv2.bucketName = BUCKET_NAME
    lorv2.prefix = prefix
    lorv2.encodingType = "" // don't use encoding

    //Starting in Spring Boot 2.6, Jackson is not able to encode the key properly if it's not
    // encoded by S3Mock. S3ObjectSummary will have empty key in this case.
    val listing = s3Client!!.listObjectsV2(lorv2)
    val summaries = listing.objectSummaries
    assertThat(summaries)
      .`as`("Must have exactly one match")
      .hasSize(1)
    assertThat(summaries[0].key)
      .`as`("Object name must match")
      .isEqualTo("")
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   */
  @Test
  fun shouldUploadAndDownloadStream() {
    s3Client!!.createBucket(BUCKET_NAME)
    val resourceId = UUID.randomUUID().toString()
    val contentEncoding = "gzip"
    val resource = byteArrayOf(1, 2, 3, 4, 5)
    val bais = ByteArrayInputStream(resource)
    val objectMetadata = ObjectMetadata()
    objectMetadata.contentLength = resource.size.toLong()
    objectMetadata.contentEncoding = contentEncoding
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, resourceId, bais, objectMetadata)
    val tm = createTransferManager()
    val upload = tm.upload(putObjectRequest)
    upload.waitForUploadResult()
    val s3Object = s3Client!!.getObject(BUCKET_NAME, resourceId)
    assertThat(s3Object.objectMetadata.contentEncoding)
      .`as`("Uploaded File should have Encoding-Type set")
      .isEqualTo(contentEncoding)
    val uploadDigest = DigestUtil.getHexDigest(ByteArrayInputStream(resource))
    val downloadedDigest = DigestUtil.getHexDigest(s3Object.objectContent)
    s3Object.close()
    assertThat(uploadDigest)
      .isEqualTo(downloadedDigest)
      .`as`("Up- and downloaded Files should have equal digests")
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  fun shouldUploadWithEncryption() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectKey = UPLOAD_FILE_NAME
    s3Client!!.createBucket(BUCKET_NAME)
    val metadata = ObjectMetadata()
    metadata.addUserMetadata("key", "value")
    val putObjectRequest =
      PutObjectRequest(BUCKET_NAME, objectKey, uploadFile).withMetadata(metadata)
    putObjectRequest.sseAwsKeyManagementParams =
      SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    s3Client!!.putObject(putObjectRequest)
    val getObjectMetadataRequest = GetObjectMetadataRequest(BUCKET_NAME, objectKey)
    val objectMetadata = s3Client!!.getObjectMetadata(getObjectMetadataRequest)
    assertThat(objectMetadata.contentLength).isEqualTo(uploadFile.length())
    assertThat(objectMetadata.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(metadata.userMetadata)
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  fun shouldNotUploadWithWrongEncryptionKey() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
    putObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
    assertThatThrownBy { s3Client!!.putObject(putObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  fun shouldNotUploadStreamingWithWrongEncryptionKey() {
    val bytes = UPLOAD_FILE_NAME.toByteArray()
    val stream: InputStream = ByteArrayInputStream(bytes)
    val objectKey = UUID.randomUUID().toString()
    s3Client!!.createBucket(BUCKET_NAME)
    val metadata = ObjectMetadata()
    metadata.contentLength = bytes.size.toLong()
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, objectKey, stream, metadata)
    putObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
    assertThatThrownBy { s3Client!!.putObject(putObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObject() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.getHexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Sourcefile and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObjectToSameKey() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3Client!!.createBucket(BUCKET_NAME)
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val copyObjectRequest = CopyObjectRequest(BUCKET_NAME, sourceKey, BUCKET_NAME, sourceKey)

    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(BUCKET_NAME, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"]).isEqualTo("test-value")

    val objectContent = copiedObject.objectContent
    val length = objectContent.available()
    assertThat(length).isEqualTo(uploadFile.length())
      .`as`("Copied item must be same length as uploaded file")

    val copiedDigest = DigestUtil.getHexDigest(objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Sourcefile and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object with REPLACE directive to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObjectWithReplaceToSameKey() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3Client!!.createBucket(BUCKET_NAME)
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val replaceObjectMetadata = ObjectMetadata()
    replaceObjectMetadata.userMetadata = mapOf("test-key2" to "test-value2")
    val copyObjectRequest = CopyObjectRequest()
      .withSourceBucketName(BUCKET_NAME)
      .withSourceKey(sourceKey)
      .withDestinationBucketName(BUCKET_NAME)
      .withDestinationKey(sourceKey)
      .withMetadataDirective(MetadataDirective.REPLACE)
      .withNewObjectMetadata(replaceObjectMetadata)

    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(BUCKET_NAME, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"])
      .`as`("Original userMetadata must have been replaced.")
      .isNullOrEmpty()
    assertThat(copiedObjectMetadata.userMetadata["test-key2"]).isEqualTo("test-value2")

    val objectContent = copiedObject.objectContent
    val length = objectContent.available()
    assertThat(length).isEqualTo(uploadFile.length())
      .`as`("Copied item must be same length as uploaded file")

    val copiedDigest = DigestUtil.getHexDigest(objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Sourcefile and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to a new bucket with new user meta data; Downloads the
   * object from the new bucket;
   * compares checksums of original and copied object; compares copied object user meta data with
   * the new user meta data specified during copy request.
   */
  @Test
  fun shouldCopyObjectWithNewUserMetadata() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val objectMetadata = ObjectMetadata()
    objectMetadata.addUserMetadata("key", "value")
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.newObjectMetadata = objectMetadata
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.getHexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
    assertThat(copiedObject.objectMetadata.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(objectMetadata.userMetadata)
  }

  /**
   * Puts an Object with some user metadata; Copies that object to a new bucket.
   * Downloads the object from the new bucket;
   * compares checksums of original and copied object; compares copied object user meta data with
   * the source object user metadata;
   */
  @Test
  fun shouldCopyObjectWithSourceUserMetadata() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey/withSourceObjectUserMetadata"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val sourceObjectMetadata = ObjectMetadata()
    sourceObjectMetadata.addUserMetadata("key", "value")
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile)
    putObjectRequest.metadata = sourceObjectMetadata
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.getHexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
    assertThat(copiedObject.objectMetadata.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(sourceObjectMetadata.userMetadata)
  }

  /**
   * Copy an object to a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  fun shouldCopyObjectToKeyNeedingEscaping() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/some escape-worthy characters %$@ $sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.getHexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Copy an object from a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  fun shouldCopyObjectFromKeyNeedingEscaping() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = "some escape-worthy characters %$@ $UPLOAD_FILE_NAME"
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.getHexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  @Throws(Exception::class)
  fun shouldCopyObjectEncrypted() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    val copyObjectResult = s3Client!!.copyObject(copyObjectRequest)
    val metadata = s3Client!!.getObjectMetadata(destinationBucketName, destinationKey)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val uploadDigest = DigestUtil.getHexDigest(TEST_ENC_KEY_ID, uploadFileIs)
    assertThat(copyObjectResult.eTag)
      .`as`("ETag should match")
      .isEqualTo(uploadDigest)
    assertThat(metadata.contentLength)
      .`as`("Files should have the same length")
      .isEqualTo(uploadFile.length())
  }

  /**
   * Tests that an object won't be copied with wrong encryption Key.
   */
  @Test
  fun shouldNotObjectCopyWithWrongEncryptionKey() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
    assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests that a copy request for a non-existing object throws the correct error.
   */
  @Test
  fun shouldThrowNoSuchKeyOnCopyForNonExistingKey() {
    val sourceKey = "NON_EXISTENT_KEY"
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.createBucket(destinationBucketName)
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404; Error Code: NoSuchKey")
  }

  /**
   * Creates a bucket and checks if it exists using [AmazonS3.doesBucketExist].
   */
  @Test
  fun bucketShouldExist() {
    s3Client!!.createBucket(BUCKET_NAME)
    val doesBucketExist = s3Client!!.doesBucketExistV2(BUCKET_NAME)
    assertThat(doesBucketExist)
      .`as`(String.format("The previously created bucket, '%s', should exist!", BUCKET_NAME))
      .isTrue
  }

  /**
   * Checks if [AmazonS3.doesBucketExistV2] is false on a not existing Bucket.
   */
  @Test
  fun bucketShouldNotExist() {
    val doesBucketExist = s3Client!!.doesBucketExistV2(BUCKET_NAME)
    assertThat(doesBucketExist)
      .`as`(String.format("The bucket, '%s', should not exist!", BUCKET_NAME))
      .isFalse
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  fun shouldGetObjectMetadata() {
    val nonExistingFileName = "nonExistingFileName"
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val objectMetadata = ObjectMetadata()
    objectMetadata.addUserMetadata("key", "value")
    objectMetadata.contentEncoding = "gzip"
    val putObjectResult = s3Client!!.putObject(
      PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
        .withMetadata(objectMetadata)
    )
    val metadataExisting = s3Client!!.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME)
    assertThat(metadataExisting.contentEncoding)
      .`as`("Content-Encoding should be identical!")
      .isEqualTo(putObjectResult.metadata.contentEncoding)
    assertThat(metadataExisting.eTag)
      .`as`("The ETags should be identical!")
      .isEqualTo(putObjectResult.eTag)
    assertThat(metadataExisting.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(objectMetadata.userMetadata)
    assertThatThrownBy {
      s3Client!!.getObjectMetadata(
        BUCKET_NAME,
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
  fun shouldDeleteObject() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    s3Client!!.deleteObject(BUCKET_NAME, UPLOAD_FILE_NAME)
    assertThatThrownBy { s3Client!!.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404")
  }

  /**
   * Tests if multiple objects can be deleted.
   */
  @Test
  fun shouldBatchDeleteObjects() {
    val uploadFile1 = File(UPLOAD_FILE_NAME)
    val uploadFile2 = File(UPLOAD_FILE_NAME)
    val uploadFile3 = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val file1 = "1_$UPLOAD_FILE_NAME"
    val file2 = "2_$UPLOAD_FILE_NAME"
    val file3 = "3_$UPLOAD_FILE_NAME"
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, file1, uploadFile1))
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, file2, uploadFile2))
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, file3, uploadFile3))
    val multiObjectDeleteRequest = DeleteObjectsRequest(BUCKET_NAME)
    val keys: MutableList<KeyVersion> = ArrayList()
    keys.add(KeyVersion(file1))
    keys.add(KeyVersion(file2))
    keys.add(KeyVersion(file3))
    multiObjectDeleteRequest.keys = keys
    val delObjRes = s3Client!!.deleteObjects(multiObjectDeleteRequest)
    assertThat(delObjRes.deletedObjects.size)
      .`as`("Response should contain 3 entries.")
      .isEqualTo(3)
    assertThat(
      delObjRes.deletedObjects.stream()
        .map { obj: DeleteObjectsResult.DeletedObject -> obj.key }
        .collect(Collectors.toList()))
      .`as`("All files are expected to be deleted.")
      .contains(file1, file2, file3)
    assertThatThrownBy { s3Client!!.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404")
  }

  /**
   * Tests if Error is thrown when DeleteObjectsRequest contains nonExisting key.
   */
  @Test
  fun shouldThrowOnBatchDeleteObjectsWrongKey() {
    val uploadFile1 = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val file1 = "1_$UPLOAD_FILE_NAME"
    val nonExistingFile = "4_" + UUID.randomUUID()
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, file1, uploadFile1))
    val multiObjectDeleteRequest = DeleteObjectsRequest(BUCKET_NAME)
    val keys: MutableList<KeyVersion> = ArrayList()
    keys.add(KeyVersion(file1))
    keys.add(KeyVersion(nonExistingFile))
    multiObjectDeleteRequest.keys = keys
    assertThatThrownBy { s3Client!!.deleteObjects(multiObjectDeleteRequest) }
      .isInstanceOf(MultiObjectDeleteException::class.java)
  }

  /**
   * Tests that a bucket can be deleted.
   */
  @Test
  fun shouldDeleteBucket() {
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.deleteBucket(BUCKET_NAME)
    val doesBucketExist = s3Client!!.doesBucketExistV2(BUCKET_NAME)
    assertThat(doesBucketExist)
      .`as`("Deleted Bucket should not exist!")
      .isFalse
  }

  /**
   * Tests that a non-empty bucket cannot be deleted.
   */
  @Test
  fun shouldNotDeleteNonEmptyBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    assertThatThrownBy { s3Client!!.deleteBucket(BUCKET_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 409; Error Code: BucketNotEmpty")
  }

  /**
   * Tests if the list objects can be retrieved.
   *
   * For more detailed tests of the List Objects API see [ListObjectIT].
   */
  @Test
  fun shouldGetObjectListing() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    val objectListingResult = s3Client!!.listObjects(BUCKET_NAME, UPLOAD_FILE_NAME)
    assertThat(objectListingResult.objectSummaries)
      .`as`("ObjectListing has no S3Objects.")
      .hasSizeGreaterThan(0)
    assertThat(objectListingResult.objectSummaries[0].key)
      .`as`("The Name of the first S3ObjectSummary item has not expected the key name.")
      .isEqualTo(UPLOAD_FILE_NAME)
  }

  /**
   * Tests if an object can be uploaded asynchronously.
   */
  @Test
  fun shouldUploadInParallel() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val transferManager = createTransferManager()
    val upload = transferManager.upload(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(UPLOAD_FILE_NAME)
    val getResult = s3Client!!.getObject(BUCKET_NAME, UPLOAD_FILE_NAME)
    assertThat(getResult.key).isEqualTo(UPLOAD_FILE_NAME)
  }

  /**
   * Verify that range-downloads work.
   */
  @Test
  fun checkRangeDownloads() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val transferManager = createTransferManager()
    val upload =
      transferManager.upload(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    upload.waitForUploadResult()
    val downloadFile = File.createTempFile(UUID.randomUUID().toString(), null)
    val download = transferManager.download(
      GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(1, 2), downloadFile
    )
    download.waitForCompletion()
    assertThat(downloadFile.length())
      .`as`("Invalid file length")
      .isEqualTo(2L)
    assertThat(download.objectMetadata.instanceLength).isEqualTo(uploadFile.length())
    assertThat(download.objectMetadata.contentLength).isEqualTo(2L)
    transferManager
      .download(
        GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(0, 1000),
        downloadFile
      )
      .waitForCompletion()
    assertThat(downloadFile.length())
      .`as`("Invalid file length")
      .isEqualTo(uploadFile.length())
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  fun multipartCopy() {
    //content larger than default part threshold of 5MiB
    val contentLen = 10 * _1MB
    val objectMetadata = ObjectMetadata()
    objectMetadata.contentLength = contentLen.toLong()
    val assumedSourceKey = UUID.randomUUID().toString()
    val sourceBucket = s3Client!!.createBucket(UUID.randomUUID().toString())
    val targetBucket = s3Client!!.createBucket(UUID.randomUUID().toString())
    val transferManager = createTransferManager()
    val upload = transferManager
      .upload(
        sourceBucket.name, assumedSourceKey,
        randomInputStream(contentLen), objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = UUID.randomUUID().toString()
    val copy = transferManager.copy(
      sourceBucket.name, assumedSourceKey, targetBucket.name,
      assumedDestinationKey
    )
    val copyResult = copy.waitForCopyResult()
    assertThat(copyResult.destinationKey).isEqualTo(assumedDestinationKey)
    assertThat(uploadResult.eTag)
      .`as`("Hashes for source and target S3Object do not match.")
      .isEqualTo(copyResult.eTag)
  }

  /**
   * Creates a bucket, stores a file, adds tags, retrieves tags and checks them for consistency.
   */
  @Test
  fun shouldAddAndRetrieveTags() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, uploadFile.name, uploadFile))
    val s3Object = s3Client!!.getObject(BUCKET_NAME, uploadFile.name)
    var getObjectTaggingRequest = GetObjectTaggingRequest(BUCKET_NAME, s3Object.key)
    var getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There shouldn't be any tags here
    assertThat(getObjectTaggingResult.tagSet).`as`("There shouldn't be any tags now")
      .hasSize(0)
    val tagList: MutableList<Tag> = ArrayList()
    tagList.add(Tag("foo", "bar"))
    val setObjectTaggingRequest =
      SetObjectTaggingRequest(BUCKET_NAME, s3Object.key, ObjectTagging(tagList))
    s3Client!!.setObjectTagging(setObjectTaggingRequest)
    getObjectTaggingRequest = GetObjectTaggingRequest(BUCKET_NAME, s3Object.key)
    getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.tagSet)
      .`as`("Couldn't find that the tag that was placed")
      .hasSize(1)
    assertThat(getObjectTaggingResult.tagSet[0].value)
      .`as`("The value of the tag placed did not match")
      .isEqualTo("bar")
  }

  /**
   * Creates a bucket, stores a file with tags, retrieves tags and checks them for consistency.
   */
  @Test
  fun canAddTagsOnPutObject() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val tagList: MutableList<Tag> = ArrayList()
    tagList.add(Tag("foo", "bar"))
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, uploadFile.name, uploadFile)
      .withTagging(ObjectTagging(tagList))
    s3Client!!.putObject(putObjectRequest)
    val s3Object = s3Client!!.getObject(BUCKET_NAME, uploadFile.name)
    val getObjectTaggingRequest = GetObjectTaggingRequest(BUCKET_NAME, s3Object.key)
    val getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.tagSet)
      .`as`("Couldn't find that the tag that was placed")
      .hasSize(1)
    assertThat(getObjectTaggingResult.tagSet[0].value)
      .`as`("The value of the tag placed did not match")
      .isEqualTo("bar")
  }

  /**
   * Creates a bucket, stores a file, get files with eTag requirements.
   */
  @Test
  @Throws(Exception::class)
  fun shouldCreateAndRespectEtag() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val returnObj = s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, uploadFile.name, uploadFile))

    // wit eTag
    var requestWithEtag = GetObjectRequest(BUCKET_NAME, uploadFile.name)
    requestWithEtag.matchingETagConstraints = listOf(returnObj.eTag)
    var requestWithHoutEtag = GetObjectRequest(BUCKET_NAME, uploadFile.name)
    // Create a new eTag that will not match
    val notEtag = returnObj.eTag.hashCode()
    requestWithHoutEtag.nonmatchingETagConstraints = listOf(notEtag.toString())
    val s3ObjectWithEtag = s3Client!!.getObject(requestWithEtag)
    val s3ObjectWithHoutEtag = s3Client!!.getObject(requestWithHoutEtag)
    val s3ObjectWithEtagDownloadedDigest = DigestUtil
      .getHexDigest(s3ObjectWithEtag.objectContent)
    val s3ObjectWithHoutEtagDownloadedDigest = DigestUtil
      .getHexDigest(s3ObjectWithHoutEtag.objectContent)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val uploadDigest = DigestUtil.getHexDigest(uploadFileIs)
    assertThat(uploadDigest)
      .`as`(
        "The uploaded file and the received file should be the same, "
          + "when requesting file with matching eTag given same eTag"
      )
      .isEqualTo(s3ObjectWithEtagDownloadedDigest)
    assertThat(uploadDigest)
      .`as`(
        "The uploaded file and the received file should be the same, "
          + "when requesting file with  non-matching eTag but given different eTag"
      )
      .isEqualTo(s3ObjectWithHoutEtagDownloadedDigest)

    // wit eTag
    requestWithEtag = GetObjectRequest(BUCKET_NAME, uploadFile.name)
    requestWithEtag.matchingETagConstraints = listOf(notEtag.toString())
    requestWithHoutEtag = GetObjectRequest(BUCKET_NAME, uploadFile.name)
    requestWithHoutEtag.nonmatchingETagConstraints = listOf(returnObj.eTag)
    val s3ObjectWithEtagNull = s3Client!!.getObject(requestWithEtag)
    val s3ObjectWithHoutEtagNull = s3Client!!.getObject(requestWithHoutEtag)
    assertThat(s3ObjectWithEtagNull)
      .`as`(
        "Get Object with matching eTag should not return object if no eTag matches"
      )
      .isNull()
    assertThat(s3ObjectWithHoutEtagNull)
      .`as`(
        "Get Object with non-matching eTag should not return object if eTag matches"
      )
      .isNull()
  }

  @Test
  @Throws(IOException::class, NoSuchAlgorithmException::class, KeyManagementException::class)
  fun generatePresignedUrlWithResponseHeaderOverrides() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, uploadFile.name, uploadFile))
    val presignedUrlRequest = GeneratePresignedUrlRequest(BUCKET_NAME, uploadFile.name)
    val overrides = ResponseHeaderOverrides()
    overrides.cacheControl = "cacheControl"
    overrides.contentDisposition = "contentDisposition"
    overrides.contentEncoding = "contentEncoding"
    overrides.contentLanguage = "contentLanguage"
    overrides.contentType = "contentType"
    overrides.expires = "expires"
    presignedUrlRequest.withResponseHeaders(overrides)
    val resourceUrl = s3Client!!.generatePresignedUrl(presignedUrlRequest)
    val urlConnection = openUrlConnection(resourceUrl)
    assertThat(urlConnection.getHeaderField(Headers.CACHE_CONTROL))
      .isEqualTo("cacheControl")
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_DISPOSITION))
      .isEqualTo("contentDisposition")
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_ENCODING))
      .isEqualTo("contentEncoding")
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_LANGUAGE))
      .isEqualTo("contentLanguage")
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_TYPE))
      .isEqualTo("contentType")
    assertThat(urlConnection.getHeaderField(Headers.EXPIRES))
      .isEqualTo("expires")
    urlConnection.getInputStream().close()
  }

  @Throws(NoSuchAlgorithmException::class, KeyManagementException::class, IOException::class)
  private fun openUrlConnection(resourceUrl: URL): URLConnection {
    val trustAllCerts = arrayOf<TrustManager>(
      object : X509TrustManager {
        override fun getAcceptedIssuers(): Array<X509Certificate> {
          return arrayOf()
        }

        override fun checkClientTrusted(certs: Array<X509Certificate>, authType: String) {}
        override fun checkServerTrusted(certs: Array<X509Certificate>, authType: String) {}
      }
    )
    val sc = SSLContext.getInstance("SSL")
    sc.init(null, trustAllCerts, SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.socketFactory)
    HttpsURLConnection.setDefaultHostnameVerifier { hostname: String, sslSession: SSLSession? -> hostname == "localhost" }
    val urlConnection = resourceUrl.openConnection()
    urlConnection.connect()
    return urlConnection
  }
}
