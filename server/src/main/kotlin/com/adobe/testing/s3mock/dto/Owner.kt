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

/**
 * Owner of a Bucket.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Owner.html)
 */
@S3Verified(year = 2025)
data class Owner(
  @Deprecated("AWS deprecated this field in 2025-05")
  @field:JsonProperty("DisplayName")
  @param:JsonProperty("DisplayName")
  val displayName: String? = null,
  @field:JsonProperty("ID")
  @param:JsonProperty("ID")
  val id: String?
) {
  companion object {
    /**
     * Default owner in S3Mock until support for ownership is implemented.
     */
    val DEFAULT_OWNER: Owner =
      Owner("s3-mock-file-store", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
    val DEFAULT_OWNER_BUCKET: Owner =
      Owner("s3-mock-file-store-bucket", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2df")
  }
}
