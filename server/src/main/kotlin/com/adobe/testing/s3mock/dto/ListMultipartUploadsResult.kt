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
 * List Multipart Uploads result.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListMultipartUploadsResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class ListMultipartUploadsResult(
  @param:JsonProperty("Bucket", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val bucket: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("CommonPrefixes", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val commonPrefixes: List<Prefix>?,
  @param:JsonProperty("Delimiter", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val delimiter: String?,
  @param:JsonProperty("EncodingType", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val encodingType: String?,
  @param:JsonProperty("IsTruncated", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val isTruncated: Boolean,
  @param:JsonProperty("KeyMarker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val keyMarker: String?,
  @param:JsonProperty("MaxUploads", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val maxUploads: Int,
  @param:JsonProperty("NextKeyMarker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val nextKeyMarker: String?,
  @param:JsonProperty("NextUploadIdMarker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val nextUploadIdMarker: String?,
  @param:JsonProperty("Prefix", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val prefix: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Upload", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val multipartUploads: List<MultipartUpload>?,
  @param:JsonProperty("UploadIdMarker", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val uploadIdMarker: String?,
)
