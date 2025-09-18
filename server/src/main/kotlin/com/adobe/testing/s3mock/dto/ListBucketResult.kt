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
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

/**
 * Represents a result of listing objects that reside in a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class ListBucketResult(
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("CommonPrefixes", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val commonPrefixes: List<Prefix>?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Contents", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val contents: List<S3Object>,
  @param:JsonProperty("Delimiter", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val delimiter: String?,
  @param:JsonProperty("EncodingType", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val encodingType: String?,
  @param:JsonProperty("IsTruncated", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val isTruncated: Boolean,
  @param:JsonProperty("Marker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val marker: String?,
  @param:JsonProperty("MaxKeys", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val maxKeys: Int,
  @param:JsonProperty("Name", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val name: String?,
  @param:JsonProperty("NextMarker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val nextMarker: String?,
  @param:JsonProperty("Prefix", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val prefix: String?,
)
