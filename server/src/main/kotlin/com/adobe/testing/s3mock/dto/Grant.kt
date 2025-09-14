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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grant.html).
 */
@JsonRootName("Grant")
@JacksonXmlRootElement(localName = "Grant")
data class Grant(
  @param:JsonProperty("Grantee")
  val grantee: Grantee?,
  @param:JsonProperty("Permission")
  val permission: Permission?
) {
  enum class Permission @JsonCreator constructor(private val value: String) {
    FULL_CONTROL("FULL_CONTROL"),
    WRITE("WRITE"),
    WRITE_ACP("WRITE_ACP"),
    READ("READ"),
    READ_ACP("READ_ACP");

    @JsonValue
    override fun toString(): String {
      return value
    }
  }
}
