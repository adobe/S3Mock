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
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public enum ChecksumMode {
  ENABLED("ENABLED"),
  DISABLED("DISABLED");

  private final String value;

  @JsonCreator
  ChecksumMode(String value) {
    this.value = value;
  }

  public static ChecksumMode fromValue(String value) {
    return switch (value) {
      case "enabled" -> ENABLED;
      case "ENABLED" -> ENABLED;
      case "disabled" -> DISABLED;
      case "DISABLED" -> DISABLED;
      default -> null;
    };
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(this.value);
  }
}
