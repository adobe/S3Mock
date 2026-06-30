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
package com.adobe.testing.s3mock.dto

import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.serialization.EtagDeserializer
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import tools.jackson.databind.annotation.JsonDeserialize

/**
 * Result to be returned when completing a multipart request.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
 */
@JsonRootName("CompleteMultipartUploadResult", namespace = S3_NS)
data class CompleteMultipartUploadResult(
  @param:JsonProperty("Bucket", namespace = S3_NS)
  val bucket: String?,
  @param:JsonProperty("ChecksumCRC32", namespace = S3_NS)
  val checksumCRC32: String? = null,
  @param:JsonProperty("ChecksumCRC32C", namespace = S3_NS)
  val checksumCRC32C: String? = null,
  @param:JsonProperty("ChecksumCRC64NVME", namespace = S3_NS)
  val checksumCRC64NVME: String? = null,
  @param:JsonProperty("ChecksumSHA1", namespace = S3_NS)
  val checksumSHA1: String? = null,
  @param:JsonProperty("ChecksumSHA256", namespace = S3_NS)
  val checksumSHA256: String? = null,
  @param:JsonProperty("ChecksumType", namespace = S3_NS)
  val checksumType: ChecksumType? = null,
  @param:JsonProperty("ETag", namespace = S3_NS)
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = S3_NS)
  val etag: String?,
  @param:JsonProperty("Key", namespace = S3_NS)
  val key: String,
  @param:JsonProperty("Location", namespace = S3_NS)
  val location: String?,
  /** Encryption headers from the originating multipart upload — echoed in the HTTP response, not serialized. */
  @field:JsonIgnore val encryptionHeaders: Map<String, String> = emptyMap(),
  @field:JsonIgnore val versionId: String?,
  @field:JsonIgnore val checksum: String? = null,
) {
  companion object {
    fun from(
      location: String?,
      bucket: String?,
      key: String,
      etag: String?,
      encryptionHeaders: Map<String, String> = emptyMap(),
      checksum: String?,
      checksumType: ChecksumType?,
      checksumAlgorithm: ChecksumAlgorithm?,
      versionId: String?,
    ): CompleteMultipartUploadResult {
      val fields = ChecksumFields.from(checksumAlgorithm, checksum)
      return CompleteMultipartUploadResult(
        bucket = bucket,
        checksumCRC32 = fields.checksumCRC32,
        checksumCRC32C = fields.checksumCRC32C,
        checksumCRC64NVME = fields.checksumCRC64NVME,
        checksumSHA1 = fields.checksumSHA1,
        checksumSHA256 = fields.checksumSHA256,
        checksumType = checksumType,
        etag = normalizeEtag(etag),
        key = key,
        location = location,
        encryptionHeaders = encryptionHeaders,
        versionId = versionId,
        checksum = checksum,
      )
    }
  }
}
