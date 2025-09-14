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
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObjectResult.html).
 */
@S3Verified(year = 2025)
@JsonRootName("CopyObjectResult")
@JacksonXmlRootElement(localName = "CopyObjectResult")
class CopyObjectResult(
  @param:JsonProperty("ChecksumCRC32")
  val checksumCRC32: String? = null,
  @param:JsonProperty("ChecksumCRC32C")
  val checksumCRC32C: String? = null,
  @param:JsonProperty("ChecksumCRC64NVME")
  val checksumCRC64NVME: String? = null,
  @param:JsonProperty("ChecksumSHA1")
  val checksumSHA1: String? = null,
  @param:JsonProperty("ChecksumSHA256")
  val checksumSHA256: String? = null,
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?,
  @JsonProperty("ETag")
  etag: String?,
  @param:JsonProperty("LastModified")
  val lastModified: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
) {
  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  constructor(metadata: S3ObjectMetadata) : this(
    if (metadata.checksumAlgorithm == ChecksumAlgorithm.CRC32) metadata.checksum else null,
    if (metadata.checksumAlgorithm == ChecksumAlgorithm.CRC32C) metadata.checksum else null,
    if (metadata.checksumAlgorithm == ChecksumAlgorithm.CRC64NVME) metadata.checksum else null,
    if (metadata.checksumAlgorithm == ChecksumAlgorithm.SHA1) metadata.checksum else null,
    if (metadata.checksumAlgorithm == ChecksumAlgorithm.SHA256) metadata.checksum else null,
    metadata.checksumType,
    metadata.etag,
    metadata.modificationDate,
  )
}
