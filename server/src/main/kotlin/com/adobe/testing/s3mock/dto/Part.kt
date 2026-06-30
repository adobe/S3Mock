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
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonDeserialize
import java.time.Instant

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Part.html).
 */
data class Part(
  @param:JsonProperty("PartNumber", namespace = S3_NS)
  val partNumber: Int,
  @param:JsonProperty("ETag", namespace = S3_NS)
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = S3_NS)
  val etag: String?,
  @param:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  @param:JsonProperty("LastModified", namespace = S3_NS)
  val lastModified: Instant,
  @param:JsonProperty("Size", namespace = S3_NS)
  val size: Long,
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
) {
  constructor(partNumber: Int, etag: String?, size: Long) :
    this(partNumber, normalizeEtag(etag), Instant.now(), size)
}
