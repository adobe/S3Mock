/*
 *  Copyright 2017-2022 Adobe.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * A DTO which can be used as a response body if an error occurred.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">API Reference</a>
 */
@JsonRootName("Error")
public class ErrorResponse {

  @JsonProperty("Code")
  private String code;

  @JsonProperty("Message")
  private String message;

  @JsonProperty("Resource")
  private String resource;

  @JsonProperty("RequestId")
  private String requestId;

  public void setCode(final String code) {
    this.code = code;
  }

  public void setMessage(final String message) {
    this.message = message;
  }

  public void setResource(final String resource) {
    this.resource = resource;
  }

  public void setRequestId(final String requestId) {
    this.requestId = requestId;
  }
}
