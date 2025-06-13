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
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.dto.VersioningConfiguration.Status
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.MultipartStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Files
import java.util.Date
import java.util.UUID

@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(types = [ObjectService::class, MultipartService::class, MultipartStore::class])
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

  @Test
  fun testListObjectsV2() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 10 //of 14
    val continuationToken: String? = null
    val fetchOwner = false
    givenBucketWithContents(bucketName, prefix)
    iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
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
      assertThat(it.contents[0].owner).isNull()
    }
  }

  @Test
  fun `list objects v2 returns owner if fetchOwner=true`() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 10 //of 14
    val continuationToken: String? = null
    val fetchOwner = true
    givenBucketWithContents(bucketName, prefix)
    iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
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
      assertThat(it.contents[0].owner).isNotNull()
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
    whenever(bucketStore.doesBucketExist(bucketName)).thenReturn(true)
    iut.verifyBucketExists(bucketName)
  }

  @Test
  fun testVerifyBucketExists_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    whenever(bucketStore.doesBucketExist(bucketName)).thenReturn(false)
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
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(metadataFrom(TEST_BUCKET_NAME))
    iut.verifyBucketIsEmpty(bucketName)
  }

  @Test
  fun testVerifyBucketIsEmpty_failure() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    whenever(bucketStore.isBucketEmpty(bucketName)).thenReturn(false)
    val bucketMetadata = BucketMetadata(
      bucketName,
      Date().toString(),
      VersioningConfiguration(null, Status.ENABLED, null),
      null,
      null,
      ObjectOwnership.BUCKET_OWNER_ENFORCED,
      Files.createTempDirectory(bucketName),
      "us-east-1",
      null,
      null,
    )

    val key = "testKey"
    val id = bucketMetadata.addKey(key)

    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(bucketMetadata)
    whenever(objectStore.getS3ObjectMetadata(bucketMetadata, id, null)).thenReturn(s3ObjectMetadata(id, key))
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
    }.isEqualTo(S3Exception.INVALID_REQUEST_MAX_KEYS)
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
      .isEqualTo(S3Exception.INVALID_REQUEST_ENCODING_TYPE)
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
  }
}
