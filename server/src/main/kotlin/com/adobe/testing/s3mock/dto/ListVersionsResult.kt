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

/**
 * Represents a result of listing object versions that reside in a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult")
data class ListVersionsResult(
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("CommonPrefixes")
  @param:JsonProperty("CommonPrefixes")
  val commonPrefixes: List<Prefix>?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("DeleteMarker")
  @param:JsonProperty("DeleteMarker")
  val deleteMarkers: List<DeleteMarkerEntry>?,
  @field:JsonProperty("Delimiter")
  @param:JsonProperty("Delimiter")
  val delimiter: String?,
  @field:JsonProperty("EncodingType")
  @param:JsonProperty("EncodingType")
  val encodingType: String?,
  @field:JsonProperty("IsTruncated")
  @param:JsonProperty("IsTruncated")
  val isTruncated: Boolean,
  @field:JsonProperty("KeyMarker")
  @param:JsonProperty("KeyMarker")
  val keyMarker: String?,
  @field:JsonProperty("MaxKeys")
  @param:JsonProperty("MaxKeys")
  val maxKeys: Int,
  @field:JsonProperty("Name")
  @param:JsonProperty("Name")
  val name: String?,
  @field:JsonProperty("NextKeyMarker")
  @param:JsonProperty("NextKeyMarker")
  val nextKeyMarker: String?,
  @field:JsonProperty("NextVersionIdMarker")
  @param:JsonProperty("NextVersionIdMarker")
  val nextVersionIdMarker: String?,
  @field:JsonProperty("Prefix")
  @param:JsonProperty("Prefix")
  val prefix: String?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("Version")
  @param:JsonProperty("Version")
  val objectVersions: List<ObjectVersion>?,
  @field:JsonProperty("VersionIdMarker")
  @param:JsonProperty("VersionIdMarker")
  val versionIdMarker: String?
)
