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
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AccessControlPolicy.html).
 * Use bean-style binding (no-args + fields) to avoid creator conflicts with @JacksonXmlElementWrapper.
 */
@S3Verified(year = 2025)
@JsonRootName("AccessControlPolicy")
@JacksonXmlRootElement(localName = "AccessControlPolicy")
class AccessControlPolicy() {
  @field:JacksonXmlElementWrapper(localName = "AccessControlList")
  @field:JacksonXmlProperty(localName = "Grant")
  @field:JsonProperty("Grant")
  var accessControlList: List<Grant>? = null

  @field:JsonProperty("Owner")
  var owner: Owner? = null

  @get:JsonIgnore
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  var xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/"

  // Convenience constructor for tests; disabled for Jackson
  @JsonCreator(mode = JsonCreator.Mode.DISABLED)
  constructor(accessControlList: List<Grant>?, owner: Owner?) : this() {
    this.owner = owner
    this.accessControlList = accessControlList
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as AccessControlPolicy

    if (accessControlList != other.accessControlList) return false
    if (owner != other.owner) return false
    if (xmlns != other.xmlns) return false

    return true
  }

  override fun hashCode(): Int {
    var result = accessControlList?.hashCode() ?: 0
    result = 31 * result + (owner?.hashCode() ?: 0)
    result = 31 * result + xmlns.hashCode()
    return result
  }
}
