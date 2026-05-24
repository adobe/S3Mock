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

import com.adobe.testing.s3mock.store.VectorBucketRecord
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonSerialize
import java.time.Instant

/**
 * Full details of a vector bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_VectorBucket.html)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class VectorBucketDetail(
  @param:JsonProperty("CreationTime")
  @param:JsonSerialize(using = InstantSerializer::class)
  val creationTime: Instant?,
  @param:JsonProperty("EncryptionConfiguration")
  val encryptionConfiguration: VectorEncryptionConfiguration?,
  @param:JsonProperty("VectorBucketArn")
  val vectorBucketArn: String?,
  @param:JsonProperty("VectorBucketName")
  val vectorBucketName: String?,
) {
  companion object {
    fun from(bucket: VectorBucketRecord): VectorBucketDetail =
      VectorBucketDetail(
        creationTime = bucket.creationTime,
        encryptionConfiguration = bucket.encryptionConfiguration,
        vectorBucketArn = bucket.vectorBucketArn,
        vectorBucketName = bucket.vectorBucketName,
      )
  }
}
