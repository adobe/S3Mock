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
import com.adobe.testing.s3mock.dto.serialization.EtagDeserializer
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonDeserialize

/**
 * Object identifier used in many APIs.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html)
 */
@S3Verified(year = 2025)
data class S3ObjectIdentifier(
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String,
  @param:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val etag: String?,
  @param:JsonProperty("LastModifiedTime", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val lastModifiedTime: String?,
  @param:JsonProperty("Size", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val size: String?,
  @param:JsonProperty("VersionId", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val versionId: String?,
)
