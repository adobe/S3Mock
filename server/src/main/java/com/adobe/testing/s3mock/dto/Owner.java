/*
 *  Copyright 2017-2024 Adobe.
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

/**
 * Owner of a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Owner.html">API Reference</a>
 */
public record Owner(
    @JsonProperty("ID")
    String id,
    @JsonProperty("DisplayName")
    String displayName
) {

  /**
   * Default owner in S3Mock until support for ownership is implemented.
   */
  public static final Owner DEFAULT_OWNER =
      new Owner("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
          "s3-mock-file-store");
  public static final Owner DEFAULT_OWNER_BUCKET =
      new Owner("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2df",
          "s3-mock-file-store-bucket");
}
