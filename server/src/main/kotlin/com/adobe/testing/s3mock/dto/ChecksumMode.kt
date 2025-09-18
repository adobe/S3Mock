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
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
 */
@S3Verified(year = 2025)
enum class ChecksumMode @JsonCreator constructor(private val value: String) {
  ENABLED("ENABLED"),
  DISABLED("DISABLED");

  @JsonValue
  override fun toString(): String {
    return this.value
  }

  companion object {
    fun fromValue(value: String): ChecksumMode? {
      return when (value) {
        "enabled" -> ENABLED
        "ENABLED" -> ENABLED
        "disabled" -> DISABLED
        "DISABLED" -> DISABLED
        else -> null
      }
    }
  }
}
