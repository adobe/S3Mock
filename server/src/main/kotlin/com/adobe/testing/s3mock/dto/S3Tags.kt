/*
 *  Copyright 2017-2026 Adobe.
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

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * S3 tags document payload represented as a JSON object.
 */
data class S3Tags(
  @param:JsonIgnore
  private val entries: MutableMap<String, String> = linkedMapOf(),
) {
  constructor(values: Map<String, String>) : this(values.toMutableMap())

  @get:JsonIgnore
  val values: Map<String, String>
    get() = entries

  @JsonAnySetter
  fun put(
    key: String,
    value: String,
  ) {
    entries[key] = value
  }

  @JsonAnyGetter
  fun asMap(): Map<String, String> = entries
}
