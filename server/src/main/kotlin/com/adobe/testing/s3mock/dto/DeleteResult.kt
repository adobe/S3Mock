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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement

/**
 * Result to be returned after batch delete request.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
 */
@JsonRootName("DeleteResult")
@JacksonXmlRootElement(localName = "DeleteResult")
data class DeleteResult(
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Deleted")
  val deletedObjects: List<DeletedS3Object>?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("Error")
  val errors: List<Error>?,
  @field:JacksonXmlProperty(isAttribute = true, localName = "xmlns")
  @get:JsonIgnore
  val xmlns: String = "http://s3.amazonaws.com/doc/2006-03-01/",
)
