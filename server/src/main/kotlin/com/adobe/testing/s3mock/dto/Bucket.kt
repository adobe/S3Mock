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
import com.adobe.testing.s3mock.store.BucketMetadata
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import java.nio.file.Path

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Bucket.html).
 */
@S3Verified(year = 2025)
data class Bucket(
  @param:JsonProperty("BucketRegion", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val bucketRegion: String?,
  @param:JsonProperty("CreationDate", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val creationDate: String?,
  @param:JsonProperty("Name", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val name: String?,
  @JsonIgnore
  val path: Path?
) {
  companion object {
    fun from(bucketMetadata: BucketMetadata): Bucket {
      return Bucket(
        bucketMetadata.bucketRegion,
        bucketMetadata.creationDate,
        bucketMetadata.name,
        bucketMetadata.path
      )
    }
  }
}
