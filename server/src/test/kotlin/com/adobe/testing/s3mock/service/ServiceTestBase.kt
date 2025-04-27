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
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.ObjectStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import org.mockito.kotlin.whenever
import org.springframework.boot.test.mock.mockito.MockBean
import software.amazon.awssdk.services.s3.model.ObjectOwnership.BUCKET_OWNER_ENFORCED
import java.nio.file.Files
import java.time.Instant
import java.util.Date
import java.util.UUID

internal abstract class ServiceTestBase {
  @MockBean
  protected lateinit var bucketStore: BucketStore

  @MockBean
  protected lateinit var objectStore: ObjectStore

  fun givenBucket(name: String): BucketMetadata {
    whenever(bucketStore.doesBucketExist(name)).thenReturn(true)
    val bucketMetadata = metadataFrom(name)
    whenever(bucketStore.getBucketMetadata(name)).thenReturn(bucketMetadata)
    return bucketMetadata
  }

  fun givenBucketWithContents(name: String, prefix: String?): List<S3Object> {
    val bucketMetadata = givenBucket(name)
    val s3Objects = givenBucketContents(prefix)
    val ids = mutableListOf<UUID>()
    for (s3Object in s3Objects) {
      val id = bucketMetadata.addKey(s3Object.key)
      ids.add(id)
      whenever(objectStore.getS3ObjectMetadata(bucketMetadata, id, null))
        .thenReturn(s3ObjectMetadata(id, s3Object.key))
    }
    whenever(bucketStore.lookupKeysInBucket(prefix, name)).thenReturn(ids)
    return s3Objects
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
    whenever(bucketStore.lookupKeysInBucket(prefix, name)).thenReturn(ids)
    return s3Objects
  }

  fun givenBucketContents(): List<S3Object> {
    return givenBucketContents(null)
  }

  fun givenBucketContents(prefix: String?): List<S3Object> {
    val list = mutableListOf<S3Object>()
    for (key in ALL_KEYS) {
      if (!prefix.isNullOrEmpty()) {
        if (!key.startsWith(prefix)) {
          continue
        }
      }
      list.add(givenS3Object(key))
    }
    return list
  }

  fun givenS3Object(key: String?): S3Object {
    val lastModified = "lastModified"
    val etag = "etag"
    val size = "size"
    val owner = Owner(0L.toString(), "name")
    return S3Object(
      key, lastModified, etag, size, StorageClass.STANDARD, owner,
      ChecksumAlgorithm.SHA256
    )
  }

  fun s3ObjectMetadata(id: UUID, key: String): S3ObjectMetadata {
    return S3ObjectMetadata(
      id,
      key,
      "size",
      "1234",
      "\"someetag\"",
      null,
      Instant.now().toEpochMilli(),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
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
      BUCKET_OWNER_ENFORCED,
      Files.createTempDirectory(bucketName)
    )
  }

  fun givenParts(count: Int, size: Long): List<Part> {
    val parts = mutableListOf<Part>()
    for (i in 1 .. count) {
      val lastModified = Date()
      parts.add(Part(i, "\"${UUID.randomUUID()}\"", lastModified, size))
    }
    return parts
  }

  companion object {
    val ALL_KEYS: Array<String> = arrayOf(
      "3330/0", "33309/0", "a",
      "b", "b/1", "b/1/1", "b/1/2", "b/2",
      "c/1", "c/1/1",
      "d:1", "d:1:1",
      "eor.txt", "foo/eor.txt"
    )
  }
}
