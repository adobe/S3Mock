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
 * Represents a result of listing objects that reside in a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult")
data class ListBucketResult(
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("CommonPrefixes")
  @param:JsonProperty("CommonPrefixes")
  val commonPrefixes: List<Prefix>?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("Contents")
  @param:JsonProperty("Contents")
  val contents: List<S3Object>,
  @field:JsonProperty("Delimiter")
  @param:JsonProperty("Delimiter")
  val delimiter: String?,
  @field:JsonProperty("EncodingType")
  @param:JsonProperty("EncodingType")
  val encodingType: String?,
  @field:JsonProperty("IsTruncated")
  @param:JsonProperty("IsTruncated")
  val isTruncated: Boolean,
  @field:JsonProperty("Marker")
  @param:JsonProperty("Marker")
  val marker: String?,
  @field:JsonProperty("MaxKeys")
  @param:JsonProperty("MaxKeys")
  val maxKeys: Int,
  @field:JsonProperty("Name")
  @param:JsonProperty("Name")
  val name: String?,
  @field:JsonProperty("NextMarker")
  @param:JsonProperty("NextMarker")
  val nextMarker: String?,
  @field:JsonProperty("Prefix")
  @param:JsonProperty("Prefix")
  val prefix: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
