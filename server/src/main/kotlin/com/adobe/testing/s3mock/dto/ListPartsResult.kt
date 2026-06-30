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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

/**
 * List-Parts result with some hard-coded values as this is sufficient for now.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListPartsResult", namespace = S3_NS)
data class ListPartsResult(
  @param:JsonProperty("Bucket", namespace = S3_NS)
  val bucket: String?,
  @param:JsonProperty("ChecksumAlgorithm", namespace = S3_NS)
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType", namespace = S3_NS)
  val checksumType: ChecksumType?,
  @param:JsonProperty("Initiator", namespace = S3_NS)
  val initiator: Initiator?,
  @param:JsonProperty("IsTruncated", namespace = S3_NS)
  val isTruncated: Boolean,
  @param:JsonProperty("Key", namespace = S3_NS)
  val key: String?,
  @param:JsonProperty("MaxParts", namespace = S3_NS)
  val maxParts: Int?,
  @param:JsonProperty("NextPartNumberMarker", namespace = S3_NS)
  val nextPartNumberMarker: Int?,
  @param:JsonProperty("Owner", namespace = S3_NS)
  val owner: Owner?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Part", namespace = S3_NS)
  val parts: List<Part>?,
  @param:JsonProperty("PartNumberMarker", namespace = S3_NS)
  val partNumberMarker: Int?,
  @param:JsonProperty("StorageClass", namespace = S3_NS)
  val storageClass: StorageClass?,
  @param:JsonProperty("UploadId", namespace = S3_NS)
  val uploadId: String?,
)
