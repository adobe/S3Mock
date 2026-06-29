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

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.Checksum
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32C
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC64NVME
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA1
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256
import com.adobe.testing.s3mock.dto.CopyObjectResult
import com.adobe.testing.s3mock.dto.DeleteMarkerEntry
import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.ObjectVersion
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.ifAlgorithm

/**
 * Extension functions that map domain model classes to DTOs.
 * Centralises model→DTO conversions so that dto/ classes have no imports of model/ classes,
 * keeping dto/ as a true leaf package.
 */

fun S3ObjectMetadata.toS3Object(): S3Object =
  S3Object(checksumAlgorithm, checksumType, etag, key, modificationDate, owner, null, size, storageClass)

fun BucketMetadata.toBucket(): Bucket =
  Bucket(
    bucketRegion,
    creationDate,
    name,
    path,
  )

fun S3ObjectMetadata.toChecksum(): Checksum? {
  val algo = checksumAlgorithm ?: return null
  return Checksum(
    algo.ifAlgorithm(CRC32, checksum),
    algo.ifAlgorithm(CRC32C, checksum),
    algo.ifAlgorithm(CRC64NVME, checksum),
    algo.ifAlgorithm(SHA1, checksum),
    algo.ifAlgorithm(SHA256, checksum),
    checksumType,
  )
}

fun S3ObjectMetadata.toObjectVersion(isLatest: Boolean): ObjectVersion =
  ObjectVersion(
    checksumAlgorithm,
    checksumType,
    normalizeEtag(etag),
    isLatest,
    key,
    modificationDate,
    owner,
    null,
    size,
    storageClass,
    versionId,
  )

fun S3ObjectMetadata.toDeleteMarkerEntry(isLatest: Boolean): DeleteMarkerEntry =
  DeleteMarkerEntry(
    isLatest,
    key,
    modificationDate,
    owner,
    versionId,
  )

fun S3ObjectMetadata.toCopyObjectResult(): CopyObjectResult =
  CopyObjectResult(
    checksumAlgorithm.ifAlgorithm(CRC32, checksum),
    checksumAlgorithm.ifAlgorithm(CRC32C, checksum),
    checksumAlgorithm.ifAlgorithm(CRC64NVME, checksum),
    checksumAlgorithm.ifAlgorithm(SHA1, checksum),
    checksumAlgorithm.ifAlgorithm(SHA256, checksum),
    checksumType,
    etag,
    modificationDate,
  )
