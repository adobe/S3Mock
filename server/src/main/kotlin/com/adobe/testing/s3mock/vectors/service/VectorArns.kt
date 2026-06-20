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
package com.adobe.testing.s3mock.vectors.service

import com.adobe.testing.s3mock.vectors.S3VectorsException

/**
 * ARN utilities for S3 Vectors resources.
 *
 * Bucket ARN: `arn:aws:s3vectors:<region>:<acct>:bucket/<name>`
 * Index ARN:  `arn:aws:s3vectors:<region>:<acct>:bucket/<bucket>/index/<index>`
 */
object VectorArns {
  private const val MOCK_ACCOUNT_ID = "123456789012"

  fun bucketArn(
    region: String,
    name: String,
  ): String = "arn:aws:s3vectors:$region:$MOCK_ACCOUNT_ID:bucket/$name"

  fun indexArn(
    region: String,
    bucketName: String,
    indexName: String,
  ): String = "arn:aws:s3vectors:$region:$MOCK_ACCOUNT_ID:bucket/$bucketName/index/$indexName"

  /** Extracts `<bucketName>` from a bucket or index ARN. */
  fun bucketNameFromArn(arn: String): String {
    val resource = arn.split(":").getOrNull(5) ?: throw S3VectorsException.validation("Invalid ARN: $arn")
    val parts = resource.split("/")
    if (parts.size < 2 || parts[0] != "bucket") throw S3VectorsException.validation("Invalid vector bucket ARN: $arn")
    return parts[1]
  }

  /** Extracts `<indexName>` from an index ARN. */
  fun indexNameFromArn(arn: String): String {
    val resource = arn.split(":").getOrNull(5) ?: throw S3VectorsException.validation("Invalid ARN: $arn")
    val parts = resource.split("/")
    if (parts.size < 4 || parts[2] != "index") throw S3VectorsException.validation("Invalid vector index ARN: $arn")
    return parts[3]
  }

  /** Returns true if `arn` looks like an ARN (starts with `arn:`). */
  fun isArn(value: String): Boolean = value.startsWith("arn:")
}
