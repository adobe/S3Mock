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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * Represents a result of listing object versions that reside in a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult")
@JacksonXmlRootElement(localName = "ListBucketResult")
data class ListVersionsResult(
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("CommonPrefixes")
  val commonPrefixes: List<Prefix>?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("DeleteMarker")
  val deleteMarkers: List<DeleteMarkerEntry>?,
  @param:JsonProperty("Delimiter")
  val delimiter: String?,
  @param:JsonProperty("EncodingType")
  val encodingType: String?,
  @param:JsonProperty("IsTruncated")
  val isTruncated: Boolean,
  @param:JsonProperty("KeyMarker")
  val keyMarker: String?,
  @param:JsonProperty("MaxKeys")
  val maxKeys: Int,
  @param:JsonProperty("Name")
  val name: String?,
  @param:JsonProperty("NextKeyMarker")
  val nextKeyMarker: String?,
  @param:JsonProperty("NextVersionIdMarker")
  val nextVersionIdMarker: String?,
  @param:JsonProperty("Prefix")
  val prefix: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Version")
  val objectVersions: List<ObjectVersion>?,
  @param:JsonProperty("VersionIdMarker")
  val versionIdMarker: String?
)
