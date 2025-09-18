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
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html).
 */
@S3Verified(year = 2025)
data class LifecycleRule(
  @param:JsonProperty("AbortIncompleteMultipartUpload", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val abortIncompleteMultipartUpload: AbortIncompleteMultipartUpload?,
  @param:JsonProperty("Expiration", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val expiration: LifecycleExpiration?,
  @param:JsonProperty("Filter", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val filter: LifecycleRuleFilter?,
  @param:JsonProperty("ID", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val id: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("NoncurrentVersionExpiration", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val noncurrentVersionExpiration: NoncurrentVersionExpiration?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("NoncurrentVersionTransition", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val noncurrentVersionTransitions: List<NoncurrentVersionTransition>?,
  @param:JsonProperty("Status", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val status: Status?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Transition", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
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
