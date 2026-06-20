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
import com.adobe.testing.s3mock.vectors.store.VectorIndexMetadata

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_Index.html)
 */
@S3Verified(year = 2026)
data class VectorIndex(
  val indexArn: String,
  val indexName: String,
  val vectorBucketName: String,
  val dataType: String,
  val dimension: Int,
  val distanceMetric: String,
  val creationTime: Double,
  val encryptionConfiguration: EncryptionConfiguration?,
  val metadataConfiguration: MetadataConfiguration?,
) {
  companion object {
    fun from(
      meta: VectorIndexMetadata,
      arn: String,
    ): VectorIndex =
      VectorIndex(
        indexArn = arn,
        indexName = meta.name,
        vectorBucketName = meta.vectorBucketName,
        dataType = meta.dataType,
        dimension = meta.dimension,
        distanceMetric = meta.distanceMetric,
        creationTime = meta.creationTime / 1000.0,
        encryptionConfiguration =
          if (meta.sseType != null) {
            EncryptionConfiguration(meta.sseType, meta.kmsKeyArn)
          } else {
            null
          },
        metadataConfiguration =
          if (meta.nonFilterableMetadataKeys.isNotEmpty()) {
            MetadataConfiguration(meta.nonFilterableMetadataKeys)
          } else {
            null
          },
      )
  }
}
