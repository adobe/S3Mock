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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html).
 */
@S3Verified(year = 2025)
@JvmRecord
data class LifecycleRule(
  @field:JsonProperty("AbortIncompleteMultipartUpload")
  @param:JsonProperty("AbortIncompleteMultipartUpload")
  val abortIncompleteMultipartUpload: AbortIncompleteMultipartUpload?,
  @field:JsonProperty("Expiration")
  @param:JsonProperty("Expiration")
  val expiration: LifecycleExpiration?,
  @field:JsonProperty("Filter")
  @param:JsonProperty("Filter")
  val filter: LifecycleRuleFilter?,
  @field:JsonProperty("ID")
  @param:JsonProperty("ID")
  val id: String?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("NoncurrentVersionExpiration")
  val noncurrentVersionExpiration: NoncurrentVersionExpiration?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("NoncurrentVersionTransition")
  @param:JsonProperty("NoncurrentVersionTransition")
  val noncurrentVersionTransitions: List<NoncurrentVersionTransition>?,
  @field:JsonProperty("Status")
  @param:JsonProperty("Status")
  val status: Status?,
  @field:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @field:JsonProperty("Transition")
  @param:JsonProperty("Transition")
  val transitions: List<Transition>?
) {
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html).
   */
  @S3Verified(year = 2025)
  enum class Status @JsonCreator constructor(private val value: String) {
    ENABLED("Enabled"),
    DISABLED("Disabled");

    @JsonValue
    override fun toString(): String {
      return value
    }
  }
}
