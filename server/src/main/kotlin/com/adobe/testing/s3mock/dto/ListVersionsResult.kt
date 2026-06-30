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
 * Represents a result of listing object versions that reside in a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult", namespace = S3_NS)
data class ListVersionsResult(
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("CommonPrefixes", namespace = S3_NS)
  val commonPrefixes: List<Prefix>?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("DeleteMarker", namespace = S3_NS)
  val deleteMarkers: List<DeleteMarkerEntry>?,
  @param:JsonProperty("Delimiter", namespace = S3_NS)
  val delimiter: String?,
  @param:JsonProperty("EncodingType", namespace = S3_NS)
  val encodingType: String?,
  @param:JsonProperty("IsTruncated", namespace = S3_NS)
  val isTruncated: Boolean,
  @param:JsonProperty("KeyMarker", namespace = S3_NS)
  val keyMarker: String?,
  @param:JsonProperty("MaxKeys", namespace = S3_NS)
  val maxKeys: Int,
  @param:JsonProperty("Name", namespace = S3_NS)
  val name: String?,
  @param:JsonProperty("NextKeyMarker", namespace = S3_NS)
  val nextKeyMarker: String?,
  @param:JsonProperty("NextVersionIdMarker", namespace = S3_NS)
  val nextVersionIdMarker: String?,
  @param:JsonProperty("Prefix", namespace = S3_NS)
  val prefix: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Version", namespace = S3_NS)
  val objectVersions: List<ObjectVersion>?,
  @param:JsonProperty("VersionIdMarker", namespace = S3_NS)
  val versionIdMarker: String?,
)
