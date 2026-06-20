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
package com.adobe.testing.s3mock.vectors.dto

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.vectors.store.VectorBucketMetadata

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_VectorBucketSummary.html)
 */
@S3Verified(year = 2026)
data class VectorBucketSummary(
  val vectorBucketArn: String,
  val vectorBucketName: String,
  val creationTime: Double,
) {
  companion object {
    fun from(
      meta: VectorBucketMetadata,
      arn: String,
    ): VectorBucketSummary =
      VectorBucketSummary(
        vectorBucketArn = arn,
        vectorBucketName = meta.name,
        creationTime = meta.creationTime / 1000.0,
      )
  }
}
