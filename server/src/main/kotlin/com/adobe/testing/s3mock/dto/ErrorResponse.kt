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
import com.fasterxml.jackson.annotation.JsonRootName

/**
 * A DTO which can be used as a response body if an error occurred.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
 */
@S3Verified(year = 2025)
@JsonRootName("Error")
@JvmRecord
data class ErrorResponse(
    @field:JsonProperty("Code")
    @param:JsonProperty("Code")
    val code: String?,
    @field:JsonProperty("Message")
    @param:JsonProperty("Message")
    val message: String?,
    @field:JsonProperty("Resource") @param:JsonProperty("Resource") val resource: String?,
    @field:JsonProperty("RequestId") @param:JsonProperty("RequestId") val requestId: String?
)
