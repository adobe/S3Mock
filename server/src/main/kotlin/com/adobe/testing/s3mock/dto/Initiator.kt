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
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Initiator.html).
 */
@S3Verified(year = 2025)
data class Initiator(
  @param:JsonProperty("DisplayName", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val displayName: String?,
  @param:JsonProperty("ID", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val id: String?
) {
  companion object {
    val DEFAULT_INITIATOR: Initiator =
      Initiator("s3-mock-file-store", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
  }
}
