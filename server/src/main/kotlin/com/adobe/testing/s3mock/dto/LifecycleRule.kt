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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record LifecycleRule(
    @JsonProperty("AbortIncompleteMultipartUpload") AbortIncompleteMultipartUpload abortIncompleteMultipartUpload,
    @JsonProperty("Expiration") LifecycleExpiration expiration,
    @JsonProperty("Filter") LifecycleRuleFilter filter,
    @JsonProperty("ID") String id,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("NoncurrentVersionExpiration") NoncurrentVersionExpiration noncurrentVersionExpiration,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("NoncurrentVersionTransition") List<NoncurrentVersionTransition> noncurrentVersionTransitions,
    @JsonProperty("Status") Status status,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Transition") List<Transition> transitions
) {

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html">API Reference</a>.
   */
  @S3Verified(year = 2025)
  public enum Status {
    ENABLED("Enabled"),
    DISABLED("Disabled");

    private final String value;

    @JsonCreator
    Status(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }
}
