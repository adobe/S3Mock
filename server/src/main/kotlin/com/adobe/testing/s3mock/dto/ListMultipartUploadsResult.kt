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
 * List Multipart Uploads result.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)
 */
@S3Verified(year = 2025)
@JsonRootName("ListMultipartUploadsResult", namespace = S3_NS)
data class ListMultipartUploadsResult(
  @param:JsonProperty("Bucket", namespace = S3_NS)
  val bucket: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("CommonPrefixes", namespace = S3_NS)
  val commonPrefixes: List<Prefix>?,
  @param:JsonProperty("Delimiter", namespace = S3_NS)
  val delimiter: String?,
  @param:JsonProperty("EncodingType", namespace = S3_NS)
  val encodingType: String?,
  @param:JsonProperty("IsTruncated", namespace = S3_NS)
  val isTruncated: Boolean,
  @param:JsonProperty("KeyMarker", namespace = S3_NS)
  val keyMarker: String?,
  @param:JsonProperty("MaxUploads", namespace = S3_NS)
  val maxUploads: Int,
  @param:JsonProperty("NextKeyMarker", namespace = S3_NS)
  val nextKeyMarker: String?,
  @param:JsonProperty("NextUploadIdMarker", namespace = S3_NS)
  val nextUploadIdMarker: String?,
  @param:JsonProperty("Prefix", namespace = S3_NS)
  val prefix: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Upload", namespace = S3_NS)
  val multipartUploads: List<MultipartUpload>?,
  @param:JsonProperty("UploadIdMarker", namespace = S3_NS)
  val uploadIdMarker: String?,
)
