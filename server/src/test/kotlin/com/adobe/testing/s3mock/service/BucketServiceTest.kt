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
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.dto.VersioningConfiguration.Status
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.kotlin.doThrow
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
    whenever(bucketStore.lookupIdsInBucket(prefix, TEST_BUCKET_NAME)).thenReturn(listOf(id))
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
  fun verifyBucketNameIsAllowed_multipleValid() {
    val max63 = "a".repeat(63)
    val samples = listOf(
      "abc",
      "a-b",
      "a.b",
      "my.bucket-name-1",
      "n0dots-or-underscores", // hyphens and digits allowed
      "a1b2c3",
      "a.b.c",
      "start1-end2",
      max63
    )
    samples.forEach { name ->
      iut.verifyBucketNameIsAllowed(name)
    }
  }

  @Test
  fun verifyBucketNameIsAllowed_multipleInvalid() {
    val tooLong = "a".repeat(64)
    val samples = listOf(
      "",                 // blank
      "a",                // too short
      "ab",               // too short
      "Aaa",              // uppercase not allowed
      "abc_",             // underscore not allowed
      "-abc",             // must start with alnum
      ".abc",             // must start with alnum
      "abc-",             // must end with alnum
      "abc.",             // must end with alnum
      "ab..cd",           // adjacent periods
      "192.168.5.4",      // formatted as IPv4
      "xn--punycode",     // forbidden prefix
      "sthree-bucket",    // forbidden prefix
      "amzn-s3-demo-foo", // forbidden prefix
      tooLong             // > 63
    )
    samples.forEach { name ->
      assertThatThrownBy { iut.verifyBucketNameIsAllowed(name) }
        .isEqualTo(S3Exception.INVALID_BUCKET_NAME)
    }
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

  internal data class Param(
    val prefix: String?,
    val delimiter: String?,
    var expectedPrefixes: Array<String> = emptyArray(),
    var expectedKeys: Array<String> = emptyArray()
  ) {
    fun prefixes(vararg expectedPrefixes: String) = apply { this.expectedPrefixes = arrayOf(*expectedPrefixes) }
    fun keys(vararg expectedKeys: String) = apply { this.expectedKeys = arrayOf(*expectedKeys) }
    override fun toString() = "prefix=$prefix, delimiter=$delimiter"
  }

  @Test
  fun testListObjectsV2_withDelimiterAndPrefix() {
    val bucketName = "bucket"
    val prefix = "b"
    val delimiter = "/"
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 100
    val continuationToken: String? = null
    val fetchOwner = false

    // provide bucket with all keys; Service will collapse common prefixes
    givenBucketWithContents(bucketName, prefix)

    val result = iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
    )

    assertThat(result.name).isEqualTo(bucketName)
    assertThat(result.prefix).isEqualTo(prefix)
    assertThat(result.delimiter).isEqualTo(delimiter)
    // With prefix "b" and delimiter "/", contents should include only key "b" and one common prefix "b/"
    assertThat(result.contents).extracting<String> { it.key }.containsExactly("b")
    assertThat(result.commonPrefixes).extracting<String> { it.prefix }.containsExactly("b/")
    assertThat(result.isTruncated).isFalse()
  }

  @Test
  fun testListObjectsV2_paginationWithContinuationToken() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 5 // smaller than available keys to force pagination
    val continuationToken: String? = null
    val fetchOwner = false

    givenBucketWithContents(bucketName, prefix)

    // first page
    val first = iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
    )

    assertThat(first.isTruncated).isTrue()
    assertThat(first.contents).hasSize(maxKeys)
    assertThat(first.nextContinuationToken).isNotBlank()

    // second page using continuation token
    val second = iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      first.nextContinuationToken,
      fetchOwner
    )

    val combined = first.contents + second.contents
    assertThat(combined).hasSizeGreaterThanOrEqualTo(maxKeys * 2)
    // total keys should not exceed all available
    assertThat(combined.map { it.key }).doesNotHaveDuplicates()
    // eventually we should reach not truncated after enough pages
    // here we only assert the API sets token on first page and can fetch subsequent page
    assertThat(second.encodingType).isEqualTo(encodingType)
  }

  @Test
  fun testListObjectsV2_withStartAfter() {
    val bucketName = "bucket"
    val prefix: String? = null
    val delimiter: String? = null
    val encodingType = "url"
    val startAfter = "b/1" // skip everything up to this key lexicographically
    val maxKeys = 100
    val continuationToken: String? = null
    val fetchOwner = false

    givenBucketWithContents(bucketName, prefix)

    val result = iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
    )

    // ensure no key before or equal to startAfter is present
    assertThat(result.contents.map { it.key }.none { it <= startAfter }).isTrue()
    assertThat(result.isTruncated).isFalse()
    assertThat(result.encodingType).isEqualTo(encodingType)
    assertThat(result.keyCount.toInt()).isEqualTo(result.contents.size)
  }

  @Test
  fun testListObjectsV2_emptyBucket() {
    val bucketName = "empty-bucket"
    val prefix: String? = null
    val delimiter = "/"
    val encodingType = "url"
    val startAfter: String? = null
    val maxKeys = 50
    val continuationToken: String? = null
    val fetchOwner = false

    // Create bucket with no contents
    givenBucketWithContents(bucketName, prefix, emptyList())

    val result = iut.listObjectsV2(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      startAfter,
      maxKeys,
      continuationToken,
      fetchOwner
    )

    assertThat(result.contents).isEmpty()
    assertThat(result.commonPrefixes).isEmpty()
    assertThat(result.isTruncated).isFalse()
    assertThat(result.nextContinuationToken).isNull()
    assertThat(result.keyCount.toInt()).isEqualTo(0)
  }

  @Test
  fun testVersioningConfiguration_getThrowsWhenAbsent_thenSetAndGet() {
    val bucketName = "bucket"
    val bucketMetadata = givenBucket(bucketName)

    // Initially absent should throw
    assertThatThrownBy { iut.getVersioningConfiguration(bucketName) }
      .isEqualTo(S3Exception.NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION)

    // Set configuration
    val cfg = VersioningConfiguration(null, Status.ENABLED, null)
    iut.setVersioningConfiguration(bucketName, cfg)

    // After setting, BucketStore should have been invoked; we simulate by making metadata return the configuration
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(
      bucketMetadata(
        bucketName,
        bucketMetadata,
        versioningConfiguration = cfg,
      )
    )

    val out = iut.getVersioningConfiguration(bucketName)
    assertThat(out.status()).isEqualTo(Status.ENABLED)
  }

  @Test
  fun testObjectLockConfiguration_getThrowsWhenAbsent_thenSetAndGet() {
    val bucketName = "bucket-lock"
    val bucketMetadata = givenBucket(bucketName)

    // Absent -> throws
    assertThatThrownBy { iut.getObjectLockConfiguration(bucketName) }
      .isEqualTo(S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK)

    // Set configuration
    val cfg = ObjectLockConfiguration(
      ObjectLockEnabled.ENABLED,
      null
    )
    iut.setObjectLockConfiguration(bucketName, cfg)

    // Return metadata updated with configuration
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(
      bucketMetadata(
        bucketName,
        bucketMetadata,
        cfg,
      )
    )

    val out = iut.getObjectLockConfiguration(bucketName)
    assertThat(out.objectLockEnabled()).isEqualTo(ObjectLockEnabled.ENABLED)
  }

  @Test
  fun testBucketLifecycleConfiguration_setGetDelete() {
    val bucketName = "bucket-lc"
    val bucketMetadata = givenBucket(bucketName)

    // Absent -> throws
    assertThatThrownBy { iut.getBucketLifecycleConfiguration(bucketName) }
      .isEqualTo(S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION)

    // Set lifecycle configuration
    val lc = BucketLifecycleConfiguration(emptyList())
    iut.setBucketLifecycleConfiguration(bucketName, lc)

    // Simulate store returning updated metadata
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(
      bucketMetadata(
        bucketName,
        bucketMetadata,
        bucketLifecycleConfiguration = lc,
      )
    )

    val read = iut.getBucketLifecycleConfiguration(bucketName)
    assertThat(read.rules()).isEmpty()

    // Delete configuration and ensure it's gone
    iut.deleteBucketLifecycleConfiguration(bucketName)

    // After delete, simulate metadata without lifecycle configuration again
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(
      bucketMetadata(bucketName, bucketMetadata)
    )

    assertThatThrownBy { iut.getBucketLifecycleConfiguration(bucketName) }
      .isEqualTo(S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION)
  }

  @Test
  fun testDeleteBucket_nonEmptyWithNonDeleteMarker_throws() {
    val bucketName = "bucket-del"
    val meta = givenBucket(bucketName)
    val key = "k1"
    val id = meta.addKey(key)

    // First call returns metadata with one object, second call also returns non-empty -> triggers exception
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(meta, meta)

    // Object metadata without delete marker
    whenever(objectStore.getS3ObjectMetadata(meta, id, null)).thenReturn(s3ObjectMetadata(id, key))

    assertThatThrownBy { iut.deleteBucket(bucketName) }
      .isInstanceOf(IllegalStateException::class.java)
      .hasMessageContaining("Bucket is not empty: $bucketName")
  }

  @Test
  fun testDeleteBucket_onlyDeleteMarkersAreRemoved_andBucketDeleted() {
    val bucketName = "bucket-del-markers"
    val metaInitial = givenBucket(bucketName)
    val key = "k1"
    val id = metaInitial.addKey(key)

    // Metadata before deletion: contains one key
    // After removing delete marker, metadata is empty
    val metaAfter = bucketMetadata(bucketName, metaInitial)

    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(metaInitial, metaAfter)

    // Return S3ObjectMetadata marked as delete marker
    val dm = s3ObjectMetadata(id, key)
    // mark it as a delete marker using helper
    val dmMeta = S3ObjectMetadata.deleteMarker(dm, "v1")

    whenever(objectStore.getS3ObjectMetadata(metaInitial, id, null)).thenReturn(dmMeta)

    // bucketStore.deleteBucket should be called and return true
    whenever(bucketStore.deleteBucket(bucketName)).thenReturn(true)

    val deleted = iut.deleteBucket(bucketName)
    assertThat(deleted).isTrue()
    // ensure we removed the key from the bucket
    verify(bucketStore).removeFromBucket(key, bucketName)
    verify(objectStore).doDeleteObject(metaInitial, id)
  }

  @Test
  fun testDeleteBucket_nonExistingBucket_throws() {
    val bucketName = "no-such-bucket"
    whenever(bucketStore.getBucketMetadata(bucketName)).doThrow(IllegalStateException(("Bucket does not exist: $bucketName")))
    assertThatThrownBy { iut.deleteBucket(bucketName) }
      .isInstanceOf(IllegalStateException::class.java)
      .hasMessageContaining("Bucket does not exist: $bucketName")
  }

  @Test
  fun testListVersions_versioningDisabled_returnsCurrentVersionsOnly() {
    val bucketName = "bucket-versions"
    val prefix = ""
    val delimiter = ""
    val encodingType = "url"
    val maxKeys = 100
    val keyMarker = ""
    val versionIdMarker = ""

    givenBucketWithContents(bucketName, prefix)

    val out = iut.listVersions(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      maxKeys,
      keyMarker,
      versionIdMarker
    )

    // With versioning disabled, entries are mapped 1:1 without delete markers
    assertThat(out.deleteMarkers()).isEmpty()
    assertThat(out.objectVersions()).isNotEmpty()
  }



  companion object {
    private const val TEST_BUCKET_NAME = "test-bucket"

    private fun bucketMetadata(
      bucketName: String,
      bucketMetadata: BucketMetadata,
      objectLockConfiguration: ObjectLockConfiguration? = bucketMetadata.objectLockConfiguration,
      bucketLifecycleConfiguration: BucketLifecycleConfiguration? = bucketMetadata.bucketLifecycleConfiguration,
      versioningConfiguration: VersioningConfiguration? = bucketMetadata.versioningConfiguration
    ): BucketMetadata {
      return BucketMetadata(
        bucketName,
        bucketMetadata.creationDate,
        versioningConfiguration,
        objectLockConfiguration,
        bucketLifecycleConfiguration,
        bucketMetadata.objectOwnership,
        bucketMetadata.path,
        bucketMetadata.bucketRegion,
        bucketMetadata.bucketInfo,
        bucketMetadata.locationInfo
      )
    }
  }
}
