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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class ListVectorsRequest
  @JsonCreator
  constructor(
    @param:JsonProperty("vectorBucketName") val vectorBucketName: String?,
    @param:JsonProperty("indexName") val indexName: String?,
    @param:JsonProperty("indexArn") val indexArn: String?,
    @param:JsonProperty("maxResults") val maxResults: Int?,
    @param:JsonProperty("nextToken") val nextToken: String?,
    @param:JsonProperty("returnData") val returnData: Boolean?,
    @param:JsonProperty("returnMetadata") val returnMetadata: Boolean?,
    @param:JsonProperty("segmentCount") val segmentCount: Int?,
    @param:JsonProperty("segmentIndex") val segmentIndex: Int?,
  )
