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
import com.adobe.testing.s3mock.dto.CopyObjectResult
import com.adobe.testing.s3mock.dto.DeleteMarkerEntry
import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.ObjectVersion
import com.adobe.testing.s3mock.dto.S3Object

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
    if (algo == ChecksumAlgorithm.CRC32) checksum else null,
    if (algo == ChecksumAlgorithm.CRC32C) checksum else null,
    if (algo == ChecksumAlgorithm.CRC64NVME) checksum else null,
    if (algo == ChecksumAlgorithm.SHA1) checksum else null,
    if (algo == ChecksumAlgorithm.SHA256) checksum else null,
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
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32C) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC64NVME) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA1) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA256) checksum else null,
    checksumType,
    etag,
    modificationDate,
  )
