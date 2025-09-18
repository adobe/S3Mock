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

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectPart.html).
 */
@S3Verified(year = 2025)
data class ObjectPart(
  @param:JsonProperty("ChecksumCRC32", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC32: String? = null,
  @param:JsonProperty("ChecksumCRC32C", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC32C: String? = null,
  @param:JsonProperty("ChecksumCRC64NVME", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumCRC64NVME: String? = null,
  @param:JsonProperty("ChecksumSHA1", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumSHA1: String? = null,
  @param:JsonProperty("ChecksumSHA256", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumSHA256: String? = null,
  @param:JsonProperty("PartNumber", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val partNumber: Int?,
  @param:JsonProperty("Size", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val size: Long?
)
