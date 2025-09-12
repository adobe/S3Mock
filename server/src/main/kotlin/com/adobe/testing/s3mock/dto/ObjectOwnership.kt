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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

/**
 * Last validation: 2025-04.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_OwnershipControlsRule.html).
 */
enum class ObjectOwnership(private val value: String) {
  BUCKET_OWNER_PREFERRED("BucketOwnerPreferred"),
  OBJECT_WRITER("ObjectWriter"),
  BUCKET_OWNER_ENFORCED("BucketOwnerEnforced");

  @JsonValue
  override fun toString(): String {
    return this.value
  }

  companion object {
    @JsonCreator
    fun fromValue(value: String): ObjectOwnership? {
      return when (value) {
        "BucketOwnerPreferred" -> BUCKET_OWNER_PREFERRED
        "ObjectWriter" -> OBJECT_WRITER
        "BucketOwnerEnforced" -> BUCKET_OWNER_ENFORCED
        else -> null
      }
    }
  }
}
