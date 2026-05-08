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

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Request body for the ListIndexes operation.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListIndexes_s3vectors.html)
 */
data class ListIndexesRequest(
  @param:JsonProperty("MaxResults")
  val maxResults: Int?,
  @param:JsonProperty("NextToken")
  val nextToken: String?,
  @param:JsonProperty("Prefix")
  val prefix: String?,
  @param:JsonProperty("VectorBucketArn")
  val vectorBucketArn: String?,
  @param:JsonProperty("VectorBucketName")
  val vectorBucketName: String?,
)
