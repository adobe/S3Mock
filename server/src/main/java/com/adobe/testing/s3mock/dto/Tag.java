/*
 *  Copyright 2017-2022 Adobe.
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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tag.html">API Reference</a>.
 */
@JsonRootName("Tag")
@JacksonXmlRootElement(localName = "Tag")
public record Tag(
    @JsonProperty("Key")
    String key,
    @JsonProperty("Value")
    String value
) {
  /**
   * Constructor for Spring's automatic header conversion.
   */
  public Tag(final String keyValuePair) {
    this(keyValuePair.split("=")[0], keyValuePair.split("=")[1]);
  }
}
