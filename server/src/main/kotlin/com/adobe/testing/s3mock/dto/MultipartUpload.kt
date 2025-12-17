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
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Date

/**
 * Container for elements related to a particular multipart upload.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_MultipartUpload.html)
 */
@S3Verified(year = 2025)
data class MultipartUpload(
  @param:JsonProperty("ChecksumAlgorithm", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumType: ChecksumType?,
  @param:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  @param:JsonProperty("Initiated", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val initiated: Date?,
  @param:JsonProperty("Initiator", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val initiator: Initiator?,
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String,
  @param:JsonProperty("Owner", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val owner: Owner,
  @param:JsonProperty("StorageClass", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val storageClass: StorageClass?,
  @param:JsonProperty("UploadId", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val uploadId: String?
)
