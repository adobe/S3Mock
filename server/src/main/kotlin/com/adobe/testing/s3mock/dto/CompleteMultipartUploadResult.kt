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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * Result to be returned when completing a multipart request.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
 */
@JsonRootName("CompleteMultipartUploadResult")
@JacksonXmlRootElement(localName = "CompleteMultipartUploadResult")
class CompleteMultipartUploadResult(
  @field:JsonProperty("Bucket")
  @param:JsonProperty("Bucket")
  val bucket: String?,
  @field:JsonProperty("ChecksumCRC32")
  @param:JsonProperty("ChecksumCRC32")
  val checksumCRC32: String? = null,
  @field:JsonProperty("ChecksumCRC32C")
  @param:JsonProperty("ChecksumCRC32C")
  val checksumCRC32C: String? = null,
  @field:JsonProperty("ChecksumCRC64NVME")
  @param:JsonProperty("ChecksumCRC64NVME")
  val checksumCRC64NVME: String? = null,
  @field:JsonProperty("ChecksumSHA1")
  @param:JsonProperty("ChecksumSHA1")
  val checksumSHA1: String? = null,
  @field:JsonProperty("ChecksumSHA256")
  @param:JsonProperty("ChecksumSHA256")
  val checksumSHA256: String? = null,
  @field:JsonProperty("ChecksumType")
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType? = null,
  @JsonProperty("ETag") etag: String?,
  @field:JsonProperty("Key")
  @param:JsonProperty("Key")
  val key: String,
  @field:JsonProperty("Location")
  @param:JsonProperty("Location")
  val location: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
  @field:JsonIgnore val multipartUploadInfo: MultipartUploadInfo,
  @field:JsonIgnore val versionId: String?,
  @field:JsonIgnore val checksum: String? = null
) {
  @JsonProperty("ETag")
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
