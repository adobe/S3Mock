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
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement
import java.time.Instant

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_S3Retention.html).
 * For unknown reasons, the timestamps in the Retention are serialized in Nanoseconds instead of
 * Milliseconds, like everywhere else.
 */
@S3Verified(year = 2025)
@JsonRootName("Retention")
@JacksonXmlRootElement(localName = "Retention")
data class Retention(
  @param:JsonProperty("Mode")
  val mode: Mode?,
  @param:JsonSerialize(using = InstantSerializer::class)
  @param:JsonDeserialize(using = InstantDeserializer::class)
  @param:JsonProperty("RetainUntilDate")
  val retainUntilDate: Instant?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
