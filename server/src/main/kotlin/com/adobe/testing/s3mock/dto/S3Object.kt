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

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.dto.serialization.EtagDeserializer
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonDeserialize

/**
 * Class representing an Object on S3.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
 */
@S3Verified(year = 2025)
data class S3Object(
  @param:JsonProperty("ChecksumAlgorithm", namespace = S3_NS)
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType", namespace = S3_NS)
  val checksumType: ChecksumType?,
  @param:JsonProperty("ETag", namespace = S3_NS)
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = S3_NS)
  val etag: String?,
  @param:JsonProperty("Key", namespace = S3_NS)
  val key: String,
  @param:JsonProperty("LastModified", namespace = S3_NS)
  val lastModified: String?,
  @param:JsonProperty("Owner", namespace = S3_NS)
  val owner: Owner?,
  @param:JsonProperty("RestoreStatus", namespace = S3_NS)
  val restoreStatus: RestoreStatus?,
  @param:JsonProperty("Size", namespace = S3_NS)
  val size: String?,
  @param:JsonProperty("StorageClass", namespace = S3_NS)
  val storageClass: StorageClass?,
)
