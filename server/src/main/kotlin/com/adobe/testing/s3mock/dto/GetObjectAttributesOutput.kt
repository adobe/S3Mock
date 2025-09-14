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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
 */
@S3Verified(year = 2025)
@JsonRootName("GetObjectAttributesOutput")
@JacksonXmlRootElement(localName = "GetObjectAttributesOutput")
data class GetObjectAttributesOutput(
  @field:JsonProperty("Checksum")
  @param:JsonProperty("Checksum")
  val checksum: Checksum?,
  @field:JsonProperty("ETag")
  @param:JsonProperty("ETag")
  val etag: String?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("ObjectParts")
  @param:JsonProperty("ObjectParts")
  val objectParts: List<GetObjectAttributesParts>?,
  @field:JsonProperty("ObjectSize")
  @param:JsonProperty("ObjectSize")
  val objectSize: Long?,
  @field:JsonProperty("StorageClass")
  @param:JsonProperty("StorageClass")
  val storageClass: StorageClass?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
) {
  fun from(metadata: S3ObjectMetadata): GetObjectAttributesOutput {
    return GetObjectAttributesOutput(
      Checksum.from(metadata),
      normalizeEtag(metadata.etag),
      null,
      metadata.size.toLong(),
      metadata.storageClass,
    )
  }
}
