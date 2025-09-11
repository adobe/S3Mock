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

package com.adobe.testing.s3mock.dto;

import com.adobe.testing.S3Verified;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * A DTO which can be used as a response body if an error occurred.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">API Reference</a>
 */
@S3Verified(year = 2025)
@JsonRootName("Error")
public record ErrorResponse(
    @JsonProperty("Code") String code,
    @JsonProperty("Message") String message,
    @JsonProperty("Resource") String resource,
    @JsonProperty("RequestId") String requestId
) {

}
