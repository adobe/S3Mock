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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32C
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC64NVME
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA1
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256
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
@JsonRootName("CompleteMultipartUploadResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class CompleteMultipartUploadResult(
  @param:JsonProperty("Bucket", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val bucket: String?,
  @param:JsonProperty("ChecksumCRC32", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC32: String? = null,
  @param:JsonProperty("ChecksumCRC32C", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC32C: String? = null,
  @param:JsonProperty("ChecksumCRC64NVME", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC64NVME: String? = null,
  @param:JsonProperty("ChecksumSHA1", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumSHA1: String? = null,
  @param:JsonProperty("ChecksumSHA256", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumSHA256: String? = null,
  @param:JsonProperty("ChecksumType", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumType: ChecksumType? = null,
  @param:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val etag: String?,
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String,
  @param:JsonProperty("Location", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
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
    ): CompleteMultipartUploadResult =
      CompleteMultipartUploadResult(
        bucket = bucket,
        checksumCRC32 = checksumAlgorithm.ifAlgorithm(CRC32, checksum),
        checksumCRC32C = checksumAlgorithm.ifAlgorithm(CRC32C, checksum),
        checksumCRC64NVME = checksumAlgorithm.ifAlgorithm(CRC64NVME, checksum),
        checksumSHA1 = checksumAlgorithm.ifAlgorithm(SHA1, checksum),
        checksumSHA256 = checksumAlgorithm.ifAlgorithm(SHA256, checksum),
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
