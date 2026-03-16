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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import tools.jackson.dataformat.xml.annotation.JacksonXmlProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CORSConfiguration.html).
 */
@JsonRootName("CORSConfiguration", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class CorsConfiguration(
  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "CORSRule", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "CORSRule", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("CORSRule", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val corsRules: List<CorsRule>?,
)

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CORSRule.html).
 */
data class CorsRule(
  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "AllowedHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "AllowedHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("AllowedHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val allowedHeaders: List<String>?,

  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "AllowedMethod", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "AllowedMethod", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("AllowedMethod", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val allowedMethods: List<String>?,

  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "AllowedOrigin", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "AllowedOrigin", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("AllowedOrigin", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val allowedOrigins: List<String>?,

  @get:JacksonXmlElementWrapper(useWrapping = false)
  @get:JacksonXmlProperty(localName = "ExposeHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JacksonXmlProperty(localName = "ExposeHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("ExposeHeader", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val exposeHeaders: List<String>?,

  @get:JacksonXmlProperty(localName = "ID", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlProperty(localName = "ID", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("ID", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val id: String?,

  @get:JacksonXmlProperty(localName = "MaxAgeSeconds", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JacksonXmlProperty(localName = "MaxAgeSeconds", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonProperty("MaxAgeSeconds", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val maxAgeSeconds: Int?,
)
