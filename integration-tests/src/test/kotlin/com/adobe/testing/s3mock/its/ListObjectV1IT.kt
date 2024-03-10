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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.transfer.TransferManager
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.File
import java.util.stream.Collectors

/**
 * Test the application using the AmazonS3 SDK V1.
 */
internal class ListObjectV1IT : S3TestBase() {

  val s3Client: AmazonS3 = createS3ClientV1()
  val transferManagerV1: TransferManager = createTransferManagerV1()

  class Param(
    val prefix: String?,
    val delimiter: String?,
    val startAfter: String?
  ) {
    var expectedKeys: Array<String?> = arrayOfNulls(0)
    var expectedPrefixes: Array<String?> = arrayOfNulls(0)
    var expectedEncoding: String? = null

    fun keys(vararg expectedKeys: String?): Param {
      this.expectedKeys = arrayOf(*expectedKeys)
      return this
    }

    fun encodedKeys(vararg expectedKeys: String): Param {
      this.expectedKeys = arrayOf(*expectedKeys)
        .map { toEncode: String? -> SdkHttpUtils.urlEncodeIgnoreSlashes(toEncode) }
        .toTypedArray()
      expectedEncoding = "url"
      return this
    }

    fun decodedKeys(): Array<String> {
      return arrayOf(*expectedKeys)
        .map { toDecode: String? -> SdkHttpUtils.urlDecode(toDecode) }
        .toTypedArray()
    }

    fun prefixes(vararg expectedPrefixes: String?): Param {
      this.expectedPrefixes = arrayOf(*expectedPrefixes)
      return this
    }

    override fun toString(): String {
      return "prefix=$prefix, delimiter=$delimiter"
    }
  }

  /**
   * Test the list V1 endpoint.
   */
  @ParameterizedTest
  @MethodSource("data")
  @S3VerifiedSuccess(year = 2022)
  fun listV1(parameters: Param, testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    // create all expected objects
    for (key in ALL_OBJECTS) {
      s3Client.putObject(bucketName, key, "Test")
    }
    val request = ListObjectsRequest(
      bucketName, parameters.prefix,
      parameters.startAfter, parameters.delimiter, null
    )
    request.encodingType = parameters.expectedEncoding
    val l = s3Client.listObjects(request)
    LOG.info(
      "list V1, prefix='{}', delimiter='{}': \n  Objects: \n    {}\n  Prefixes: \n    {}\n",  //
      parameters.prefix,  //
      parameters.delimiter,  //
      l.objectSummaries.stream().map { obj: S3ObjectSummary -> obj.key }
        .collect(Collectors.joining("\n    ")),  //
      java.lang.String.join("\n    ", l.commonPrefixes) //
    )
    var expectedPrefixes = parameters.expectedPrefixes
    // AmazonS3#listObjects does not decode the prefixes, need to encode expected values
    if (parameters.expectedEncoding != null) {
      expectedPrefixes = arrayOf(*parameters.expectedPrefixes)
        .map { toEncode: String? -> SdkHttpUtils.urlEncodeIgnoreSlashes(toEncode) }
        .toTypedArray()
    }
    assertThat(l.objectSummaries.stream()
      .map { obj: S3ObjectSummary -> obj.key }
      .collect(Collectors.toList())
    ).containsExactlyInAnyOrderElementsOf(listOf(*parameters.expectedKeys))
    assertThat(ArrayList(l.commonPrefixes)).containsExactlyInAnyOrderElementsOf(listOf(*expectedPrefixes))
    assertThat(l.encodingType).isEqualTo(parameters.expectedEncoding)
  }

  /**
   * Test the list V2 endpoint.
   */
  @ParameterizedTest
  @MethodSource("data")
  @S3VerifiedSuccess(year = 2022)
  fun listV2(parameters: Param, testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    // create all expected objects
    for (key in ALL_OBJECTS) {
      s3Client.putObject(bucketName, key, "Test")
    }
    val l = s3Client.listObjectsV2(
      ListObjectsV2Request()
        .withBucketName(bucketName)
        .withDelimiter(parameters.delimiter)
        .withPrefix(parameters.prefix)
        .withStartAfter(parameters.startAfter)
        .withEncodingType(parameters.expectedEncoding)
    )
    LOG.info("list V2, prefix='{}', delimiter='{}', startAfter='{}': Objects: {} Prefixes: {}",
      parameters.prefix,
      parameters.delimiter,
      parameters.startAfter,
      l.objectSummaries.stream().map { s: S3ObjectSummary -> SdkHttpUtils.urlDecode(s.key) }
        .collect(Collectors.joining("\n    ")),
      java.lang.String.join("\n    ", l.commonPrefixes)
    )
    // listV2 automatically decodes the keys so the expected keys have to be decoded
    val expectedDecodedKeys = parameters.decodedKeys()
    assertThat(l.objectSummaries.stream()
      .map { obj: S3ObjectSummary -> obj.key }
      .collect(Collectors.toList())
    ).containsExactlyInAnyOrderElementsOf(listOf(*expectedDecodedKeys))
    // AmazonS3#listObjectsV2 returns decoded prefixes
    assertThat(ArrayList(l.commonPrefixes)).containsExactlyInAnyOrderElementsOf(listOf(*parameters.expectedPrefixes))
    assertThat(l.encodingType).isEqualTo(parameters.expectedEncoding)
  }

  /**
   * Uses weird, but valid characters in the key used to store an object. Verifies
   * that ListObject returns the correct object names.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldListWithCorrectObjectNames(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val weirdStuff = ("$&_ .,':\u0001") // use only characters that are safe or need special handling
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = prefix + weirdStuff + uploadFile.name + weirdStuff
    s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))
    val listing = s3Client.listObjects(bucketName, prefix)
    val summaries = listing.objectSummaries
    assertThat(summaries).hasSize(1)
    assertThat(summaries[0].key).isEqualTo(key)
  }

  /**
   * Same as [shouldListWithCorrectObjectNames] but for V2 API.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldListV2WithCorrectObjectNames(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val weirdStuff = ("$&_ .,':\u0001") // use only characters that are safe or need special handling
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = prefix + weirdStuff + uploadFile.name + weirdStuff
    s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))

    // AWS client ListObjects V2 defaults to no encoding whereas V1 defaults to URL
    val request = ListObjectsV2Request()
    request.bucketName = bucketName
    request.prefix = prefix
    request.encodingType = "url" // do use encoding!
    val listing = s3Client.listObjectsV2(request)
    val summaries = listing.objectSummaries
    assertThat(summaries).hasSize(1)
    assertThat(summaries[0].key).isEqualTo(key)
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldHonorEncodingType(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val prefix = "shouldHonorEncodingType/"
    val key = prefix + "\u0001" // key invalid in XML
    s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))
    val lor = ListObjectsRequest(bucketName, prefix, null, null, null)
    lor.encodingType = "url"

    val listing = s3Client.listObjects(lor)
    val summaries = listing.objectSummaries
    assertThat(summaries).hasSize(1)
    assertThat(summaries[0].key).isEqualTo("shouldHonorEncodingType/%01")
  }

  /**
   * The same as [shouldHonorEncodingType] but for V2 API.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldHonorEncodingTypeV2(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val prefix = "shouldHonorEncodingType/"
    val key = prefix + "\u0001" // key invalid in XML
    s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))
    val request = ListObjectsV2Request()
    request.bucketName = bucketName
    request.prefix = prefix
    request.encodingType = "url"

    val listing = s3Client.listObjectsV2(request)
    val summaries = listing.objectSummaries
    assertThat(summaries).hasSize(1)
    assertThat(summaries[0].key).isEqualTo("shouldHonorEncodingType/\u0001")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldGetObjectListing(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client.putObject(PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile))
    val objectListingResult = s3Client.listObjects(bucketName, UPLOAD_FILE_NAME)
    assertThat(objectListingResult.objectSummaries).hasSizeGreaterThan(0)
    assertThat(objectListingResult.objectSummaries[0].key).isEqualTo(UPLOAD_FILE_NAME)
  }

  /**
   * Stores files in a previously created bucket. List files using ListObjectsV2Request
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldUploadAndListV2Objects(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client.putObject(
      PutObjectRequest(
        bucketName,
        uploadFile.name, uploadFile
      )
    )
    s3Client.putObject(
      PutObjectRequest(
        bucketName,
        uploadFile.name + "copy1", uploadFile
      )
    )
    s3Client.putObject(
      PutObjectRequest(
        bucketName,
        uploadFile.name + "copy2", uploadFile
      )
    )
    val request = ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(3)
    val listResult = s3Client.listObjectsV2(request)
    assertThat(listResult.keyCount).isEqualTo(3)
    for (objectSummary in listResult.objectSummaries) {
      assertThat(objectSummary.key).contains(uploadFile.name)
      val s3Object = s3Client.getObject(bucketName, objectSummary.key)
      verifyObjectContent(uploadFile, s3Object)
    }
  }

  companion object {
    private val ALL_OBJECTS = arrayOf(
      "3330/0", "33309/0", "a",
      "b", "b/1", "b/1/1", "b/1/2", "b/2",
      "c/1", "c/1/1",
      "d:1", "d:1:1",
      "eor.txt", "foo/eor.txt"
    )

    private fun param(prefix: String?, delimiter: String?, startAfter: String?): Param {
      return Param(prefix, delimiter, startAfter)
    }

    /**
     * Parameter factory.
     */
    @JvmStatic
    fun data(): Iterable<Param> {
      return listOf( //
        param(null, null, null).keys(*ALL_OBJECTS),  //
        param("", null, null).keys(*ALL_OBJECTS),  //
        param(null, "", null).keys(*ALL_OBJECTS),  //
        param(null, "/", null).keys("a", "b", "d:1", "d:1:1", "eor.txt")
          .prefixes("3330/", "foo/", "c/", "b/", "33309/"),
        param("", "", null).keys(*ALL_OBJECTS),  //
        param("/", null, null),  //
        param("b", null, null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),  //
        param("b/", null, null).keys("b/1", "b/1/1", "b/1/2", "b/2"),  //
        param("b", "", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),  //
        param("b", "/", null).keys("b").prefixes("b/"),  //
        param("b/", "/", null).keys("b/1", "b/2").prefixes("b/1/"),  //
        param("b/1", "/", null).keys("b/1").prefixes("b/1/"),  //
        param("b/1/", "/", null).keys("b/1/1", "b/1/2"),  //
        param("c", "/", null).prefixes("c/"),  //
        param("c/", "/", null).keys("c/1").prefixes("c/1/"),  //
        param("eor", "/", null).keys("eor.txt"),  //
        // start after existing key
        param("b", null, "b/1/1").keys("b/1/2", "b/2"),  //
        // start after non-existing key
        param("b", null, "b/0").keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("3330/", null, null).keys("3330/0"),
        param(null, null, null).encodedKeys(*ALL_OBJECTS),
        param("b/1", "/", null).encodedKeys("b/1").prefixes("b/1/")
      )
    }
  }
}
