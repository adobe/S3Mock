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
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * List-Parts result with some hard-coded values as this is sufficient for now.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListPartsResult")
@JacksonXmlRootElement(localName = "ListPartsResult")
data class ListPartsResult(
  @param:JsonProperty("Bucket")
  val bucket: String?,
  @param:JsonProperty("ChecksumAlgorithm")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?,
  @param:JsonProperty("Initiator")
  val initiator: Owner?,
  @param:JsonProperty("IsTruncated")
  val isTruncated: Boolean,
  @param:JsonProperty("Key")
  val key: String?,
  @param:JsonProperty("MaxParts")
  val maxParts: Int?,
  @param:JsonProperty("NextPartNumberMarker")
  val nextPartNumberMarker: Int?,
  @param:JsonProperty("Owner")
  val owner: Owner?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Part")
  val parts: List<Part>?,
  @param:JsonProperty("PartNumberMarker")
  val partNumberMarker: Int?,
  @param:JsonProperty("StorageClass")
  val storageClass: StorageClass?,
  @param:JsonProperty("UploadId")
  val uploadId: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
