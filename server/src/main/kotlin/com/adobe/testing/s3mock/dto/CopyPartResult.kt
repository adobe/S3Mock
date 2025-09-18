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

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import java.util.Date

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyPartResult.html).
 */
@S3Verified(year = 2025)
@JsonRootName("CopyPartResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
class CopyPartResult(
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
  @JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  etag: String?,
  @param:JsonProperty("LastModified", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  val lastModified: Date?,
) {
  @JsonIgnore
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  constructor(
    date: Date?, etag: String?
  ) : this(
    null,
    null,
    null,
    null,
    null,
    etag,
    date,
  )

  constructor(
    checksumAlgorithm: ChecksumAlgorithm?,
    checksum: String?,
    etag: String?,
    lastModified: Date?
  ) : this(
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32C) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC64NVME) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA1) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA256) checksum else null,
    etag,
    lastModified,
  )

  companion object {
    fun from(date: Date?, etag: String?): CopyPartResult {
      return CopyPartResult(date, etag)
    }
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as CopyPartResult

    if (checksumCRC32 != other.checksumCRC32) return false
    if (checksumCRC32C != other.checksumCRC32C) return false
    if (checksumCRC64NVME != other.checksumCRC64NVME) return false
    if (checksumSHA1 != other.checksumSHA1) return false
    if (checksumSHA256 != other.checksumSHA256) return false
    if (lastModified != other.lastModified) return false
    if (etag != other.etag) return false

    return true
  }

  override fun hashCode(): Int {
    var result = checksumCRC32?.hashCode() ?: 0
    result = 31 * result + (checksumCRC32C?.hashCode() ?: 0)
    result = 31 * result + (checksumCRC64NVME?.hashCode() ?: 0)
    result = 31 * result + (checksumSHA1?.hashCode() ?: 0)
    result = 31 * result + (checksumSHA256?.hashCode() ?: 0)
    result = 31 * result + (lastModified?.hashCode() ?: 0)
    result = 31 * result + (etag?.hashCode() ?: 0)
    return result
  }
}
