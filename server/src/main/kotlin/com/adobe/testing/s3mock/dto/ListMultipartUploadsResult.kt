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
 * List Multipart Uploads result.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListMultipartUploadsResult")
data class ListMultipartUploadsResult(
  @field:JsonProperty("Bucket")
  @param:JsonProperty("Bucket")
  val bucket: String?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("CommonPrefixes")
  @param:JsonProperty("CommonPrefixes")
  val commonPrefixes: List<Prefix>?,
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
  @field:JsonProperty("MaxUploads")
  @param:JsonProperty("MaxUploads")
  val maxUploads: Int,
  @field:JsonProperty("NextKeyMarker")
  @param:JsonProperty("NextKeyMarker")
  val nextKeyMarker: String?,
  @field:JsonProperty("NextUploadIdMarker")
  @param:JsonProperty("NextUploadIdMarker")
  val nextUploadIdMarker: String?,
  @field:JsonProperty("Prefix")
  @param:JsonProperty("Prefix")
  val prefix: String?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("Upload")
  @param:JsonProperty("Upload")
  val multipartUploads: List<MultipartUpload>?,
  @field:JsonProperty("UploadIdMarker")
  @param:JsonProperty("UploadIdMarker")
  val uploadIdMarker: String?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
