/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.model

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

class MappersTest {
  @Test
  fun `BucketMetadata toBucket maps all fields`() {
    val meta =
      BucketMetadata(
        name = "test-bucket",
        creationDate = "2026-01-01T00:00:00.000Z",
        versioningConfiguration = null,
        objectLockConfiguration = null,
        bucketLifecycleConfiguration = null,
        objectOwnership = null,
        path = Path.of("/tmp/test-bucket"),
        bucketRegion = "us-east-1",
        bucketInfo = null,
        locationInfo = null,
      )
    val bucket = meta.toBucket()
    assertThat(bucket.name).isEqualTo("test-bucket")
    assertThat(bucket.creationDate).isEqualTo("2026-01-01T00:00:00.000Z")
    assertThat(bucket.bucketRegion).isEqualTo("us-east-1")
    assertThat(bucket.path).isEqualTo(Path.of("/tmp/test-bucket"))
  }

  @Test
  fun `S3ObjectMetadata toChecksum returns null when no algorithm`() {
    val meta = minimalS3ObjectMetadata()
    assertThat(meta.toChecksum()).isNull()
  }

  @Test
  fun `S3ObjectMetadata toChecksum populates correct field for SHA256`() {
    val meta =
      minimalS3ObjectMetadata().copy(
        checksumAlgorithm = ChecksumAlgorithm.SHA256,
        checksumType = ChecksumType.FULL_OBJECT,
        checksum = "abc123",
      )
    val checksum = meta.toChecksum()
    assertThat(checksum).isNotNull()
    assertThat(checksum!!.checksumSHA256).isEqualTo("abc123")
    assertThat(checksum.checksumCRC32).isNull()
    assertThat(checksum.checksumType).isEqualTo(ChecksumType.FULL_OBJECT)
  }

  @Test
  fun `S3ObjectMetadata toObjectVersion maps all fields`() {
    val meta =
      minimalS3ObjectMetadata().copy(
        etag = "myetag",
        versionId = "v1",
      )
    val version = meta.toObjectVersion(isLatest = true)
    assertThat(version.key).isEqualTo("test-key")
    assertThat(version.isLatest).isTrue()
    assertThat(version.versionId).isEqualTo("v1")
    // etag normalization applied
    assertThat(version.etag).isEqualTo("\"myetag\"")
  }

  @Test
  fun `S3ObjectMetadata toDeleteMarkerEntry maps all fields`() {
    val meta = minimalS3ObjectMetadata().copy(versionId = "v2")
    val entry = meta.toDeleteMarkerEntry(isLatest = false)
    assertThat(entry.key).isEqualTo("test-key")
    assertThat(entry.isLatest).isFalse()
    assertThat(entry.versionId).isEqualTo("v2")
    assertThat(entry.owner).isEqualTo(Owner.DEFAULT_OWNER)
  }

  @Test
  fun `S3ObjectMetadata toS3Object maps all fields`() {
    val meta =
      minimalS3ObjectMetadata().copy(
        etag = "myetag",
        checksumAlgorithm = ChecksumAlgorithm.SHA256,
        checksumType = ChecksumType.FULL_OBJECT,
      )
    val s3Object = meta.toS3Object()
    assertThat(s3Object.key).isEqualTo("test-key")
    assertThat(s3Object.size).isEqualTo("0")
    assertThat(s3Object.storageClass).isEqualTo(StorageClass.STANDARD)
    assertThat(s3Object.checksumAlgorithm).isEqualTo(ChecksumAlgorithm.SHA256)
    assertThat(s3Object.checksumType).isEqualTo(ChecksumType.FULL_OBJECT)
    assertThat(s3Object.owner).isEqualTo(Owner.DEFAULT_OWNER)
    // restoreStatus is null (not a field on S3ObjectMetadata)
    assertThat(s3Object.restoreStatus).isNull()
  }

  @Test
  fun `S3ObjectMetadata toCopyObjectResult maps checksum and etag`() {
    val meta =
      minimalS3ObjectMetadata().copy(
        checksumAlgorithm = ChecksumAlgorithm.CRC32,
        checksum = "deadbeef",
        etag = "myetag",
        modificationDate = "2026-01-01T00:00:00.000Z",
      )
    val result = meta.toCopyObjectResult()
    assertThat(result.checksumCRC32).isEqualTo("deadbeef")
    assertThat(result.checksumSHA256).isNull()
    // etag normalization applied by primary constructor
    assertThat(result.etag).isEqualTo("\"myetag\"")
    assertThat(result.lastModified).isEqualTo("2026-01-01T00:00:00.000Z")
  }

  private fun minimalS3ObjectMetadata() =
    S3ObjectMetadata(
      id = UUID.randomUUID(),
      key = "test-key",
      size = "0",
      modificationDate = "2026-01-01T00:00:00.000Z",
      etag = null,
      contentType = "text/plain",
      lastModified = 0L,
      dataPath = Path.of("/tmp"),
      legalHold = null,
      retention = null,
      owner = Owner.DEFAULT_OWNER,
      checksumAlgorithm = null,
      checksum = null,
      storageClass = StorageClass.STANDARD,
      policy = null,
      versionId = null,
      checksumType = null,
    )
}
