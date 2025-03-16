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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.store.MultipartStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import java.util.UUID
import java.util.stream.Collectors

@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockBean(classes = [ObjectService::class, MultipartService::class, MultipartStore::class])
internal class BucketServiceTest : ServiceTestBase() {
  @Autowired
  private lateinit var iut: BucketService

  @Test
  fun getObject() {
    assertPrefix("a/b/c", "a/b/c")
  }

  @Test
  fun getObjectsForParentDirectory() {
    assertPrefix("a/b/c", "a/b")
  }

  @Test
  fun getObjectsForPartialPrefix() {
    assertPrefix("foo_bar_baz", "foo")
  }

  @Test
  fun getObjectsForEmptyPrefix() {
    assertPrefix("a", "")
  }

  @Test
  fun getObjectsForNullPrefix() {
    assertPrefix("a", null)
  }

  @Test
  fun getObjectsForPartialParentDirectory() {
    assertPrefix("a/bee/c", "a/b")
  }

  private fun assertPrefix(key: String, prefix: String?) {
    val id = UUID.randomUUID()
    whenever(bucketStore.lookupKeysInBucket(prefix, TEST_BUCKET_NAME)).thenReturn(listOf(id))
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    whenever(bucketStore.getBucketMetadata(TEST_BUCKET_NAME)).thenReturn(bucketMetadata)
    whenever(objectStore.getS3ObjectMetadata(bucketMetadata, id, null)).thenReturn(s3ObjectMetadata(id, key))
    val result = iut.getS3Objects(TEST_BUCKET_NAME, prefix)
    assertThat(result).hasSize(1)
    assertThat(result[0].key).isEqualTo(key)
  }

  @ParameterizedTest
  @MethodSource("data")
  fun testCommonPrefixesAndBucketContentFilter(parameters: Param) {
    val prefix = parameters.prefix
    val delimiter = parameters.delimiter
    val bucketContents = givenBucketContents(prefix)
    val commonPrefixes = BucketService.collapseCommonPrefixes(prefix, delimiter, bucketContents)

    val filteredBucketContents =
      BucketService.filterObjectsBy(bucketContents, commonPrefixes)

    val expectedPrefixes = parameters.expectedPrefixes
    val expectedKeys = parameters.expectedKeys

    assertThat(commonPrefixes).hasSize(expectedPrefixes.size)
      .containsExactlyInAnyOrderElementsOf(expectedPrefixes.toList())

    assertThat(filteredBucketContents.stream().map(S3Object::key).collect(Collectors.toList()))
      .containsExactlyInAnyOrderElementsOf(expectedKeys.toList())
  }

  @Test
  fun testCommonPrefixesNoPrefixNoDelimiter() {
    val prefix = ""
    val delimiter = ""
    val bucketContents = givenBucketContents()

    val commonPrefixes = BucketService.collapseCommonPrefixes(prefix, delimiter, bucketContents)
    assertThat(commonPrefixes).isEmpty()
  }

  @Test
  fun testCommonPrefixesPrefixNoDelimiter() {
    val prefix = "prefix-a"
    val delimiter = ""
    val bucketContents = givenBucketContents()

    val commonPrefixes = BucketService.collapseCommonPrefixes(prefix, delimiter, bucketContents)
    assertThat(commonPrefixes).isEmpty()
  }

  @Test
  fun testCommonPrefixesNoPrefixDelimiter() {
    val prefix = ""
    val delimiter = "/"
    val bucketContents = givenBucketContents()

    val commonPrefixes = BucketService.collapseCommonPrefixes(prefix, delimiter, bucketContents)
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/")
  }

  @Test
  fun testCommonPrefixesPrefixDelimiter() {
    val prefix = "3330"
    val delimiter = "/"
    val bucketContents = givenBucketContents()

    val commonPrefixes = BucketService.collapseCommonPrefixes(prefix, delimiter, bucketContents)
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/")
  }

  @Test
  fun testListObjectsV2() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 10 //of 14
    val continuationToken: String? = null
    givenBucketWithContents(bucketName, prefix)
    iut.listObjectsV2(
      bucketName, prefix, delimiter, encodingType, startAfter, maxKeys,
      continuationToken
    ).also {
      assertThat(it).isNotNull()
      assertThat(it.name).isEqualTo(bucketName)
      assertThat(it.prefix).isEqualTo(prefix)
      assertThat(it.startAfter).isEqualTo(startAfter)
      assertThat(it.encodingType).isEqualTo(encodingType)
      assertThat(it.isTruncated).isTrue()
      assertThat(it.maxKeys).isEqualTo(maxKeys)
      assertThat(it.nextContinuationToken).isNotEmpty()
      assertThat(it.contents).hasSize(maxKeys)
    }
  }

  @Test
  fun testListObjectsV1() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val marker: String? = null
    val encodingType = "url"
    val maxKeys = 10 //of 14
    givenBucketWithContents(bucketName, prefix)
    iut.listObjectsV1(bucketName, prefix, delimiter, marker, encodingType, maxKeys).also {
      assertThat(it).isNotNull()
      assertThat(it.name).isEqualTo(bucketName)
      assertThat(it.prefix).isEqualTo(prefix)
      assertThat(it.marker).isEqualTo(marker)
      assertThat(it.encodingType).isEqualTo(encodingType)
      assertThat(it.isTruncated).isTrue()
      assertThat(it.maxKeys).isEqualTo(maxKeys)
      assertThat(it.nextMarker).isEqualTo("c/1/1")
      assertThat(it.contents).hasSize(maxKeys)
    }
  }

  @Test
  fun testVerifyBucketExists_success() {
    val bucketName = "bucket"
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(metadataFrom(TEST_BUCKET_NAME))
    iut.verifyBucketExists(bucketName)
  }

  @Test
  fun testVerifyBucketExists_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(null)
    assertThatThrownBy { iut.verifyBucketExists(bucketName) }
      .isEqualTo(S3Exception.NO_SUCH_BUCKET)
  }

  @Test
  fun testVerifyBucketObjectLockEnabled_success() {
    val bucketName = "bucket"
    whenever(bucketStore.isObjectLockEnabled(bucketName)).thenReturn(true)
    iut.verifyBucketObjectLockEnabled(bucketName)
  }

  @Test
  fun testVerifyBucketObjectLockEnabled_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    whenever(bucketStore.isObjectLockEnabled(bucketName)).thenReturn(false)
    assertThatThrownBy { iut.verifyBucketObjectLockEnabled(bucketName) }
      .isEqualTo(S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK)
  }

  @Test
  fun testVerifyBucketNameIsAllowed_success() {
    val bucketName = "bucket"
    iut.verifyBucketNameIsAllowed(bucketName)
  }

  @Test
  fun testVerifyBucketNameIsAllowed_failure() {
    val bucketName = "!!!bucketNameNotAllowed!!!"
    givenBucket(bucketName)
    assertThatThrownBy { iut.verifyBucketNameIsAllowed(bucketName) }
      .isEqualTo(S3Exception.INVALID_BUCKET_NAME)
  }

  @Test
  fun testVerifyBucketDoesNotExist_success() {
    val bucketName = "bucket"
    iut.verifyBucketDoesNotExist(bucketName)
    verify(bucketStore).doesBucketExist(bucketName)
  }

  @Test
  fun testVerifyBucketDoesNotExist_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    assertThatThrownBy { iut.verifyBucketDoesNotExist(bucketName) }
      .isEqualTo(S3Exception.BUCKET_ALREADY_OWNED_BY_YOU)
  }

  @Test
  fun testVerifyBucketIsEmpty_success() {
    val bucketName = "bucket"
    whenever(bucketStore.isBucketEmpty(bucketName)).thenReturn(true)
    iut.verifyBucketIsEmpty(bucketName)
  }

  @Test
  fun testVerifyBucketIsEmpty_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    whenever(bucketStore.isBucketEmpty(bucketName)).thenReturn(false)
    assertThatThrownBy { iut.verifyBucketIsEmpty(bucketName) }
      .isEqualTo(S3Exception.BUCKET_NOT_EMPTY)
  }

  @Test
  fun testVerifyMaxKeys_success() {
    val keys = 10
    iut.verifyMaxKeys(keys)
  }

  @Test
  fun testVerifyMaxKeys_failure() {
    val keys = -1
    assertThatThrownBy {
      iut.verifyMaxKeys(keys)
    }.isEqualTo(S3Exception.INVALID_REQUEST_MAXKEYS)
  }

  @Test
  fun testVerifyEncodingType_success() {
    val encodingType = "url"
    iut.verifyEncodingType(encodingType)
  }

  @Test
  fun testVerifyEncodingType_failure() {
    val encodingType = "not-url"
    assertThatThrownBy { iut.verifyEncodingType(encodingType) }
      .isEqualTo(S3Exception.INVALID_REQUEST_ENCODINGTYPE)
  }

  internal class Param(val prefix: String?, val delimiter: String?) {
    var expectedPrefixes: Array<String> = arrayOf()
    var expectedKeys: Array<String> = arrayOf()

    fun prefixes(vararg expectedPrefixes: String): Param {
      this.expectedPrefixes = arrayOf(*expectedPrefixes)
      return this
    }

    fun keys(vararg expectedKeys: String): Param {
      this.expectedKeys = arrayOf(*expectedKeys)
      return this
    }

    override fun toString(): String {
      return "prefix=$prefix, delimiter=$delimiter"
    }
  }

  companion object {
    private const val TEST_BUCKET_NAME = "test-bucket"

    /**
     * Parameter factory.
     * Taken from ListObjectIT to make sure we unit test against the same data.
     */
    @JvmStatic
    fun data(): Iterable<Param> {
      return listOf(
        param(null, null).keys(*ALL_KEYS),
        param("", null).keys(*ALL_KEYS),
        param(null, "").keys(*ALL_KEYS),
        param(null, "/").keys("a", "b", "d:1", "d:1:1", "eor.txt")
          .prefixes("3330/", "foo/", "c/", "b/", "33309/"),
        param("", "").keys(*ALL_KEYS),
        param("/", null),
        param("b", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b/", null).keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "").keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "/").keys("b").prefixes("b/"),
        param("b/", "/").keys("b/1", "b/2").prefixes("b/1/"),
        param("b/1", "/").keys("b/1").prefixes("b/1/"),
        param("b/1/", "/").keys("b/1/1", "b/1/2"),
        param("c", "/").prefixes("c/"),
        param("c/", "/").keys("c/1").prefixes("c/1/"),
        param("eor", "/").keys("eor.txt")
      )
    }

    private fun param(prefix: String?, delimiter: String?): Param {
      return Param(prefix, delimiter)
    }
  }
}
