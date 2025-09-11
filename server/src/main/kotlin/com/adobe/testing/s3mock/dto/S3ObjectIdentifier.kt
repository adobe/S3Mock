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
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Object identifier used in many APIs.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html)
 */
@S3Verified(year = 2025)
class S3ObjectIdentifier(
  @field:JsonProperty("Key")
  @param:JsonProperty("Key")
  val key: String,
  @JsonProperty("ETag") etag: String?,
  @field:JsonProperty("LastModifiedTime")
  @param:JsonProperty("LastModifiedTime")
  val lastModifiedTime: String?,
  @field:JsonProperty("Size")
  @param:JsonProperty("Size")
  val size: String?,
  @field:JsonProperty("VersionId")
  @param:JsonProperty("VersionId")
  val versionId: String?
) {
  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as S3ObjectIdentifier

    if (key != other.key) return false
    if (lastModifiedTime != other.lastModifiedTime) return false
    if (size != other.size) return false
    if (versionId != other.versionId) return false
    if (etag != other.etag) return false

    return true
  }

  override fun hashCode(): Int {
    var result = key.hashCode()
    result = 31 * result + (lastModifiedTime?.hashCode() ?: 0)
    result = 31 * result + (size?.hashCode() ?: 0)
    result = 31 * result + (versionId?.hashCode() ?: 0)
    result = 31 * result + (etag?.hashCode() ?: 0)
    return result
  }


}
