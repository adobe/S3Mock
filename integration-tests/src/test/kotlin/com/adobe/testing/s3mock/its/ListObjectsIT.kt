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
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.ObjectStorageClass
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.utils.http.SdkHttpUtils

internal class ListObjectsIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObjectsListObjectsV2_checksumAlgorithm_sha256(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-1")
        it.checksumAlgorithm(ChecksumAlgorithm.SHA256)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.putObject(
      {
        it.bucket(bucketName).key("$UPLOAD_FILE_NAME-2")
        it.checksumAlgorithm(ChecksumAlgorithm.SHA256)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
      }.also {
        assertThat(it.contents())
          .hasSize(2)
          .extracting(S3Object::checksumAlgorithm)
          .containsOnly(
            Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
            Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
          )
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObjectsListObjectsV1_checksumAlgorithm_sha256(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName).key("$UPLOAD_FILE_NAME-1")
        it.checksumAlgorithm(ChecksumAlgorithm.SHA256)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.putObject(
      {
        it.bucket(bucketName).key("$UPLOAD_FILE_NAME-2")
        it.checksumAlgorithm(ChecksumAlgorithm.SHA256)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjects {
        it.bucket(bucketName)
      }.also {
        assertThat(it.contents())
          .hasSize(2)
          .extracting(S3Object::checksumAlgorithm)
          .containsOnly(
            Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
            Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
          )
        // ListObjects returns the default storageClass "STANDARD", even though other APIs may not.
        assertThat(it.contents())
          .hasSize(2)
          .extracting(S3Object::storageClass)
          .containsOnly(
            Tuple(ObjectStorageClass.STANDARD),
            Tuple(ObjectStorageClass.STANDARD),
          )
      }
  }

  /**
   * Test list with safe characters in keys.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun shouldListV1WithCorrectObjectNames(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val weirdStuff = charsSafe()
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = "$prefix$weirdStuff$UPLOAD_FILE_NAME$weirdStuff"
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjects {
        it.bucket(bucketName)
        it.prefix(prefix)
        it.encodingType(EncodingType.URL)
      }.also { listing ->
        listing.contents().also {
          assertThat(it).hasSize(1)
          assertThat(it[0].key()).isEqualTo(key)
        }
      }
  }

  /**
   * Test list with safe characters in keys.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun shouldListV2WithCorrectObjectNames(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val weirdStuff = charsSafe()
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = "$prefix$weirdStuff$UPLOAD_FILE_NAME$weirdStuff"
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.prefix(prefix)
        it.encodingType(EncodingType.URL)
      }.also { listing ->
        listing.contents().also {
          assertThat(it).hasSize(1)
          assertThat(it[0].key()).isEqualTo(key)
          // ListObjectsV2 returns the default storageClass "STANDARD", even though other APIs may not.
          assertThat(it[0].storageClass()).isEqualTo(ObjectStorageClass.STANDARD)
        }
      }
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
  @S3VerifiedSuccess(year = 2025)
  fun shouldHonorEncodingTypeV1(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val weirdStuff = "\u0001" // key invalid in XML
    val prefix = "shouldHonorEncodingTypeV1/"
    val key = "$prefix$weirdStuff$UPLOAD_FILE_NAME$weirdStuff"
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjects {
        it.bucket(bucketName)
        it.prefix(prefix)
        it.encodingType(EncodingType.URL)
      }.also { listing ->
        listing.contents().also {
          assertThat(it).hasSize(1)
          assertThat(it[0].key()).isEqualTo(key)
        }
      }
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
  @S3VerifiedSuccess(year = 2025)
  fun shouldHonorEncodingTypeV2(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val weirdStuff = "\u0001" // key invalid in XML
    val prefix = "shouldHonorEncodingTypeV2/"
    val key = "$prefix$weirdStuff$UPLOAD_FILE_NAME$weirdStuff"
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.prefix(prefix)
        it.encodingType(EncodingType.URL)
      }.also { listing ->
        listing.contents().also {
          assertThat(it).hasSize(1)
          assertThat(it[0].key()).isEqualTo(key)
        }
      }
  }

  @ParameterizedTest
  @MethodSource("data")
  @S3VerifiedSuccess(year = 2025)
  fun listV1(
    parameters: Param,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)

    for (key in ALL_OBJECTS) {
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(key)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }

    // listV2 automatically decodes the keys so the expected keys have to be decoded
    val expectedDecodedKeys = parameters.decodedKeys()

    s3Client
      .listObjects {
        it.bucket(bucketName)
        it.prefix(parameters.prefix)
        it.delimiter(parameters.delimiter)
        it.marker(parameters.startAfter)
        it.encodingType(parameters.expectedEncoding)
      }.also { listing ->
        LOG.info(
          "list V1, prefix='{}', delimiter='{}', startAfter='{}': Objects: {} Prefixes: {}",
          parameters.prefix,
          parameters.delimiter,
          parameters.startAfter,
          listing.contents().joinToString("\n    ") { s: S3Object -> SdkHttpUtils.urlDecode(s.key()) },
          listing.commonPrefixes().joinToString("\n    ", transform = CommonPrefix::prefix),
        )
        listing.commonPrefixes().also {
          assertThat(it.map { s: CommonPrefix -> SdkHttpUtils.urlDecode(s.prefix()) })
            .containsExactlyInAnyOrder(*parameters.expectedPrefixes)
        }
        listing.contents().also {
          assertThat(it.map { s: S3Object -> SdkHttpUtils.urlDecode(s.key()) }).isEqualTo(listOf(*expectedDecodedKeys))
        }
        if (parameters.expectedEncoding != null) {
          assertThat(listing.encodingType().toString()).isEqualTo(parameters.expectedEncoding)
        } else {
          assertThat(listing.encodingType()).isNull()
        }
      }
  }

  @ParameterizedTest
  @MethodSource("data")
  @S3VerifiedSuccess(year = 2025)
  fun listV2(
    parameters: Param,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)

    for (key in ALL_OBJECTS) {
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(key)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }

    // listV2 automatically decodes the keys so the expected keys have to be decoded
    val expectedDecodedKeys = parameters.decodedKeys()

    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.prefix(parameters.prefix)
        it.delimiter(parameters.delimiter)
        it.startAfter(parameters.startAfter)
        it.encodingType(parameters.expectedEncoding)
      }.also { listing ->
        LOG.info(
          "list V2, prefix='{}', delimiter='{}', startAfter='{}': Objects: {} Prefixes: {}",
          parameters.prefix,
          parameters.delimiter,
          parameters.startAfter,
          listing.contents().joinToString("\n    ") { s: S3Object -> SdkHttpUtils.urlDecode(s.key()) },
          listing.commonPrefixes().joinToString("\n    ", transform = CommonPrefix::prefix),
        )
        listing.commonPrefixes().also {
          assertThat(it.map { s: CommonPrefix -> SdkHttpUtils.urlDecode(s.prefix()) })
            .containsExactlyInAnyOrder(*parameters.expectedPrefixes)
        }
        listing.contents().also {
          assertThat(it.map { s: S3Object -> SdkHttpUtils.urlDecode(s.key()) }).isEqualTo(listOf(*expectedDecodedKeys))
        }
        if (parameters.expectedEncoding != null) {
          assertThat(listing.encodingType().toString()).isEqualTo(parameters.expectedEncoding)
        } else {
          assertThat(listing.encodingType()).isNull()
        }
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsLimitedAmountOfObjectsBasedOnMaxKeys(testInfo: TestInfo) {
    val (bucketName, keys) = givenBucketAndObjects(testInfo, 30)
    val maxKeys = 10
    val listedObjects = mutableListOf<String>()

    val continuationToken1 =
      s3Client
        .listObjectsV2 {
          it.bucket(bucketName)
          it.maxKeys(maxKeys)
        }.let { listing ->
          assertThat(listing.contents().size).isEqualTo(maxKeys)
          assertThat(listing.isTruncated).isTrue
          assertThat(listing.maxKeys()).isEqualTo(maxKeys)
          assertThat(listing.nextContinuationToken()).isNotNull
          listedObjects.addAll(listing.contents().map(S3Object::key))
          listing.nextContinuationToken()
        }

    val continuationToken2 =
      s3Client
        .listObjectsV2 {
          it.bucket(bucketName)
          it.maxKeys(maxKeys)
          it.continuationToken(continuationToken1)
        }.let { listing ->
          assertThat(listing.contents().size).isEqualTo(maxKeys)
          assertThat(listing.isTruncated).isTrue
          assertThat(listing.maxKeys()).isEqualTo(maxKeys)
          assertThat(listing.nextContinuationToken()).isNotNull
          listedObjects.addAll(listing.contents().map(S3Object::key))
          listing.nextContinuationToken()
        }

    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.maxKeys(maxKeys)
        it.continuationToken(continuationToken2)
      }.also { listing ->
        assertThat(listing.contents().size).isEqualTo(maxKeys)
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(maxKeys)
        assertThat(listing.nextContinuationToken()).isNull()
        listedObjects.addAll(listing.contents().map(S3Object::key))
      }

    assertThat(listedObjects).hasSize(30)
    assertThat(listedObjects).hasSameElementsAs(keys)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsAllObjectsIfMaxKeysIsDefault(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjects(testInfo, 30)
    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
      }.also { listing ->
        assertThat(listing.contents().size).isEqualTo(30)
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(1000)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsAllObjectsIfMaxKeysEqualToAmountOfObjects(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjects(testInfo, 30)
    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.maxKeys(30)
      }.also { listing ->
        assertThat(listing.contents().size).isEqualTo(30)
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(30)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsAllObjectsIfMaxKeysMoreThanAmountOfObjects(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjects(testInfo, 30)
    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.maxKeys(400)
      }.also { listing ->
        assertThat(listing.contents().size).isEqualTo(30)
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(400)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsEmptyListIfMaxKeysIsZero(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjects(testInfo, 30)
    s3Client
      .listObjects {
        it.bucket(bucketName)
        it.maxKeys(0)
      }.also { listing ->
        assertThat(listing.contents()).isEmpty()
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(0)
        assertThat(listing.nextMarker()).isNull()
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun returnsEmptyListIfMaxKeysIsZeroV2(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjects(testInfo, 30)
    s3Client
      .listObjectsV2 {
        it.bucket(bucketName)
        it.maxKeys(0)
      }.also { listing ->
        assertThat(listing.contents()).isEmpty()
        assertThat(listing.isTruncated).isFalse
        assertThat(listing.maxKeys()).isEqualTo(0)
        assertThat(listing.nextContinuationToken()).isNull()
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun listObjects_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listObjects {
        it.bucket(randomName)
        it.prefix(UPLOAD_FILE_NAME)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  companion object {
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
    private val ALL_OBJECTS =
      arrayOf(
        "3330/0",
        "33309/0",
        "a",
        "b",
        "b/1",
        "b/1/1",
        "b/1/2",
        "b/2",
        "c/1",
        "c/1/1",
        "d:1",
        "d:1:1",
        "eor.txt",
        "foo/eor.txt",
      )

    private fun param(
      prefix: String?,
      delimiter: String?,
      startAfter: String?,
    ): Param = Param(prefix, delimiter, startAfter)

    /**
     * Parameter factory.
     */
    @JvmStatic
    fun data(): Iterable<Param> =
      listOf( //
        param(null, null, null).keys(*ALL_OBJECTS), //
        param("", null, null).keys(*ALL_OBJECTS), //
        param(null, "", null).keys(*ALL_OBJECTS), //
        param(null, "/", null)
          .keys("a", "b", "d:1", "d:1:1", "eor.txt")
          .prefixes("3330/", "foo/", "c/", "b/", "33309/"),
        param("", "", null).keys(*ALL_OBJECTS), //
        param("/", null, null), //
        param("b", null, null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b/", null, null).keys("b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b", "", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b", "/", null).keys("b").prefixes("b/"), //
        param("b/", "/", null).keys("b/1", "b/2").prefixes("b/1/"), //
        param("b/1", "/", null).keys("b/1").prefixes("b/1/"), //
        param("b/1/", "/", null).keys("b/1/1", "b/1/2"), //
        param("c", "/", null).prefixes("c/"), //
        param("c/", "/", null).keys("c/1").prefixes("c/1/"), //
        param("eor", "/", null).keys("eor.txt"), //
        // start after existing key
        param("b", null, "b/1/1").keys("b/1/2", "b/2"), //
        // start after non-existing key
        param("b", null, "b/0").keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("3330/", null, null).keys("3330/0"),
        param(null, null, null).encodedKeys(*ALL_OBJECTS),
        param("b/1", "/", null).encodedKeys("b/1").prefixes("b/1/"),
      )

    class Param(
      val prefix: String?,
      val delimiter: String?,
      val startAfter: String?,
    ) {
      var expectedKeys: Array<String?> = arrayOfNulls(0)
      var expectedPrefixes: Array<String?> = arrayOfNulls(0)
      var expectedEncoding: String? = null

      fun keys(vararg expectedKeys: String?): Param {
        this.expectedKeys = arrayOf(*expectedKeys)
        return this
      }

      fun encodedKeys(vararg expectedKeys: String): Param {
        this.expectedKeys =
          arrayOf(*expectedKeys)
            .map { toEncode: String? -> SdkHttpUtils.urlEncodeIgnoreSlashes(toEncode) }
            .toTypedArray()
        expectedEncoding = "url"
        return this
      }

      fun decodedKeys(): Array<String> =
        arrayOf(*expectedKeys)
          .map { toDecode: String? -> SdkHttpUtils.urlDecode(toDecode) }
          .toTypedArray()

      fun prefixes(vararg expectedPrefixes: String?): Param {
        this.expectedPrefixes = arrayOf(*expectedPrefixes)
        return this
      }

      override fun toString(): String = "prefix=$prefix, delimiter=$delimiter"
    }
  }
}
