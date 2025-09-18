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
package com.adobe.testing.s3mock.dto

import com.adobe.testing.s3mock.store.MultipartUploadInfo
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName

/**
 * Result to be returned when completing a multipart request.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
 */
@JsonRootName("CompleteMultipartUploadResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
class CompleteMultipartUploadResult(
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
  @JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  etag: String?,
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String,
  @param:JsonProperty("Location", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val location: String?,
  @field:JsonIgnore val multipartUploadInfo: MultipartUploadInfo,
  @field:JsonIgnore val versionId: String?,
  @field:JsonIgnore val checksum: String? = null
) {
  @JsonIgnore
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }
  companion object {
    fun from(
      location: String?,
      bucket: String?,
      key: String,
      etag: String?,
      multipartUploadInfo: MultipartUploadInfo,
      checksum: String?,
      checksumType: ChecksumType?,
      checksumAlgorithm: ChecksumAlgorithm?,
      versionId: String?
    ): CompleteMultipartUploadResult {
      val usedAlgorithm = checksumAlgorithm ?: multipartUploadInfo.checksumAlgorithm
      val usedChecksum = checksum ?: multipartUploadInfo.checksum

      if (usedChecksum == null) {
        return CompleteMultipartUploadResult(
          bucket = bucket,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
        )
      }

      return when (usedAlgorithm) {
        ChecksumAlgorithm.CRC32 -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumCRC32 = usedChecksum,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
          checksum = checksum
        )

        ChecksumAlgorithm.CRC32C -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumCRC32C = usedChecksum,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
          checksum = checksum
        )

        ChecksumAlgorithm.CRC64NVME -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumCRC64NVME = usedChecksum,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
          checksum = checksum
        )

        ChecksumAlgorithm.SHA1 -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumSHA1 = usedChecksum,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
          checksum = checksum
        )

        ChecksumAlgorithm.SHA256 -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumSHA256 = usedChecksum,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
          checksum = checksum
        )

        else -> CompleteMultipartUploadResult(
          bucket = bucket,
          checksumType = checksumType,
          etag = etag,
          key = key,
          location = location,
          multipartUploadInfo = multipartUploadInfo,
          versionId = versionId,
        )
      }
    }
  }
}
