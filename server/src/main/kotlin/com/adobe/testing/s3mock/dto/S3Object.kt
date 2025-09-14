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
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Class representing an Object on S3.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
 */
@S3Verified(year = 2025)
class S3Object (
  @param:JsonProperty("ChecksumAlgorithm")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?,
  @JsonProperty("ETag") etag: String?,
  @param:JsonProperty("Key")
  val key: String,
  @param:JsonProperty("LastModified")
  val lastModified: String?,
  @param:JsonProperty("Owner")
  val owner: Owner?,
  @param:JsonProperty("RestoreStatus")
  val restoreStatus: RestoreStatus?,
  @param:JsonProperty("Size")
  val size: String?,
  @param:JsonProperty("StorageClass")
  val storageClass: StorageClass?
) {
  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  companion object {
    fun from(s3ObjectMetadata: S3ObjectMetadata): S3Object {
      return S3Object(
        s3ObjectMetadata.checksumAlgorithm,
        s3ObjectMetadata.checksumType,
        s3ObjectMetadata.etag,
        s3ObjectMetadata.key,
        s3ObjectMetadata.modificationDate,
        s3ObjectMetadata.owner,
        null,
        s3ObjectMetadata.size,
        s3ObjectMetadata.storageClass
      )
    }
  }
}
