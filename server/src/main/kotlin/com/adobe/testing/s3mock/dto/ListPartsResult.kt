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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty

/**
 * List-Parts result with some hard-coded values as this is sufficient for now.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListPartsResult")
data class ListPartsResult(
  @field:JsonProperty("Bucket")
  @param:JsonProperty("Bucket")
  val bucket: String?,
  @field:JsonProperty("ChecksumAlgorithm")
  @param:JsonProperty("ChecksumAlgorithm")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @field:JsonProperty("ChecksumType")
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?,
  @field:JsonProperty("Initiator")
  @param:JsonProperty("Initiator")
  val initiator: Owner?,
  @field:JsonProperty("IsTruncated")
  @param:JsonProperty("IsTruncated")
  val isTruncated: Boolean,
  @field:JsonProperty("Key")
  @param:JsonProperty("Key")
  val key: String?,
  @field:JsonProperty("MaxParts")
  @param:JsonProperty("MaxParts")
  val maxParts: Int?,
  @field:JsonProperty("NextPartNumberMarker")
  @param:JsonProperty("NextPartNumberMarker")
  val nextPartNumberMarker: Int?,
  @field:JsonProperty("Owner")
  @param:JsonProperty("Owner")
  val owner: Owner?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("Part")
  @param:JsonProperty("Part")
  val parts: List<Part>?,
  @field:JsonProperty("PartNumberMarker")
  @param:JsonProperty("PartNumberMarker")
  val partNumberMarker: Int?,
  @field:JsonProperty("StorageClass")
  @param:JsonProperty("StorageClass")
  val storageClass: StorageClass?,
  @field:JsonProperty("UploadId")
  @param:JsonProperty("UploadId")
  val uploadId: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
