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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.service.BucketServiceTest.Param
import com.adobe.testing.s3mock.service.ServiceBase.Companion.filterBy
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.ObjectStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.whenever
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Date
import java.util.UUID

internal abstract class ServiceTestBase {
  @MockitoBean
  protected lateinit var bucketStore: BucketStore

  @MockitoBean
  protected lateinit var objectStore: ObjectStore

  @ParameterizedTest
  @MethodSource("data")
  fun testCommonPrefixesAndBucketContentFilter(parameters: Param) {
    val prefix = parameters.prefix
    val delimiter = parameters.delimiter
    val bucketContents = givenBucketContents(prefix)
    val commonPrefixes = ServiceBase.collapseCommonPrefixes(prefix, delimiter, bucketContents, S3Object::key)

    val filteredBucketContents = filterBy(bucketContents, S3Object::key, commonPrefixes)

    val expectedPrefixes = parameters.expectedPrefixes
    val expectedKeys = parameters.expectedKeys

    assertThat(commonPrefixes).hasSize(expectedPrefixes.size)
      .containsExactlyInAnyOrderElementsOf(expectedPrefixes.toList())

    assertThat(filteredBucketContents.map(S3Object::key))
      .containsExactlyInAnyOrderElementsOf(expectedKeys.toList())
  }

  @Test
  fun testCommonPrefixesNoPrefixNoDelimiter() {
    val prefix = ""
    val delimiter = ""
    val bucketContents = givenBucketContents()

    val commonPrefixes = ServiceBase.collapseCommonPrefixes(prefix, delimiter, bucketContents, S3Object::key)
    assertThat(commonPrefixes).isEmpty()
  }

  @Test
  fun testCommonPrefixesPrefixNoDelimiter() {
    val prefix = "prefix-a"
    val delimiter = ""
    val bucketContents = givenBucketContents()

    val commonPrefixes = ServiceBase.collapseCommonPrefixes(prefix, delimiter, bucketContents, S3Object::key)
    assertThat(commonPrefixes).isEmpty()
  }

  @Test
  fun testCommonPrefixesNoPrefixDelimiter() {
    val prefix = ""
    val delimiter = "/"
    val bucketContents = givenBucketContents()

    val commonPrefixes = ServiceBase.collapseCommonPrefixes(prefix, delimiter, bucketContents, S3Object::key)
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/")
  }

  @Test
  fun testCommonPrefixesPrefixDelimiter() {
    val prefix = "3330"
    val delimiter = "/"
    val bucketContents = givenBucketContents()

    val commonPrefixes = ServiceBase.collapseCommonPrefixes(prefix, delimiter, bucketContents, S3Object::key)
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/")
  }


  fun givenBucket(name: String): BucketMetadata {
    whenever(bucketStore.doesBucketExist(name)).thenReturn(true)
    val bucketMetadata = metadataFrom(name)
    whenever(bucketStore.getBucketMetadata(name)).thenReturn(bucketMetadata)
    return bucketMetadata
  }

  fun givenBucketWithContents(name: String, prefix: String?): List<S3Object> {
    val s3Objects = givenBucketContents(prefix)
    return givenBucketWithContents(name, prefix, s3Objects)
  }

  fun givenBucketWithContents(name: String, prefix: String?, s3Objects: List<S3Object>): List<S3Object> {
    val bucketMetadata = givenBucket(name)
    val ids = mutableListOf<UUID>()
    for (s3Object in s3Objects) {
      val id = bucketMetadata.addKey(s3Object.key)
      ids.add(id)
      whenever(objectStore.getS3ObjectMetadata(bucketMetadata, id, null))
        .thenReturn(s3ObjectMetadata(id, s3Object.key))
    }
    whenever(bucketStore.lookupIdsInBucket(prefix, name)).thenReturn(ids)
    return s3Objects
  }

  fun givenBucketContents(): List<S3Object> = givenBucketContents(null)

  fun givenBucketContents(prefix: String?): List<S3Object> {
    return ALL_KEYS
      .asSequence()
      .filter { key -> prefix.isNullOrEmpty() || key.startsWith(prefix) }
      .map { key -> givenS3Object(key) }
      .toList()
  }

  fun givenS3Object(key: String): S3Object {
    val lastModified = "lastModified"
    val etag = "etag"
    val size = "size"
    val owner = Owner(0L.toString())
    return S3Object(
      ChecksumAlgorithm.SHA256,
      ChecksumType.FULL_OBJECT,
      etag,
      key,
      lastModified,
      owner,
      null,
      size,
      StorageClass.STANDARD
    )
  }

  fun s3ObjectMetadata(id: UUID, key: String): S3ObjectMetadata {
    val lastModified = "lastModified"
    val etag = "etag"
    val size = "size"
    val owner = Owner(0L.toString())
    return S3ObjectMetadata(
      id,
      key,
      size,
      lastModified,
      "\"$etag\"",
      null,
      Instant.now().toEpochMilli(),
      Path.of("test"),
      null,
      null,
      null,
      null,
      owner,
      null,
      null,
      ChecksumAlgorithm.SHA256,
      null,
      StorageClass.STANDARD,
      null,
      null,
      false,
      ChecksumType.FULL_OBJECT
    )
  }

  fun metadataFrom(bucketName: String): BucketMetadata {
    return BucketMetadata(
      bucketName,
      Date().toString(),
      null,
      null,
      null,
      ObjectOwnership.BUCKET_OWNER_ENFORCED,
      Files.createTempDirectory(bucketName),
      "us-east-1",
      null,
      null,
    )
  }

  fun givenParts(count: Int, size: Long): List<Part> = (1..count).map {
    Part(it, "\"${UUID.randomUUID()}\"", Date(), size)
  }

  companion object {
    val ALL_KEYS: Array<String> = arrayOf(
      "3330/0", "33309/0", "a",
      "b", "b/1", "b/1/1", "b/1/2", "b/2",
      "c/1", "c/1/1",
      "d:1", "d:1:1",
      "eor.txt", "foo/eor.txt"
    )

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
