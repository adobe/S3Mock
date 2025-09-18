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
import com.fasterxml.jackson.annotation.JsonRootName
import tools.jackson.databind.annotation.JsonDeserialize
import tools.jackson.databind.annotation.JsonSerialize
import tools.jackson.dataformat.xml.annotation.JacksonXmlText

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html).
 */
@S3Verified(year = 2025)
@JsonRootName("LocationConstraint", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class LocationConstraint(
  @param:JsonSerialize(using = RegionSerializer::class)
  @param:JsonDeserialize(using = RegionDeserializer::class)
  @param:JacksonXmlText
  @get:JsonSerialize(using = RegionSerializer::class)
  @get:JsonDeserialize(using = RegionDeserializer::class)
  @get:JacksonXmlText
  val region: Region?,
) {
  @JsonCreator
  constructor(region: String?) : this(Region.fromValue(region))
}
