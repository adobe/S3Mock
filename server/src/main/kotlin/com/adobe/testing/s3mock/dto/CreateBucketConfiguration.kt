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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucketConfiguration.html).
 */
@S3Verified(year = 2025)
@JsonRootName("CreateBucketConfiguration")
data class CreateBucketConfiguration(
  @field:JsonProperty("Bucket")
  @param:JsonProperty("Bucket")
  val bucket: BucketInfo?,
  @field:JsonProperty("Location")
  @param:JsonProperty("Location")
  val location: LocationInfo?,
  @field:JsonProperty("LocationConstraint")
  @field:JsonDeserialize(using = LocationConstraintDeserializer::class)
  @field:JsonSerialize(using = LocationConstraintSerializer::class)
  @param:JsonSerialize(using = LocationConstraintSerializer::class)
  @param:JsonDeserialize(using = LocationConstraintDeserializer::class)
  @param:JsonProperty("LocationConstraint")
  val locationConstraint: LocationConstraint?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @param:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
) {
  fun regionFrom(): String? {
    if (this.locationConstraint != null
      && this.locationConstraint.region != null
    ) {
      return this.locationConstraint.region.toString()
    }
    return null
  }
}
