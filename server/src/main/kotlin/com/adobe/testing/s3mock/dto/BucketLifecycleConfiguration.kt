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
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_BucketLifecycleConfiguration.html).
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleConfiguration.html).
 */
@S3Verified(year = 2025)
@JsonRootName("LifecycleConfiguration")
data class BucketLifecycleConfiguration(
  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "Rule")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "Rule")
  @param:JsonProperty("rules")
  val rules: List<LifecycleRule>?,

  @get:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JsonProperty("xmlns")
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
