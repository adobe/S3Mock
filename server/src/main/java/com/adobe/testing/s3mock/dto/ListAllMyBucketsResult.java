/*
 *  Copyright 2017-2024 Adobe.
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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Represents a result of listing all Buckets.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html">API Reference</a>
 */
@JsonRootName("ListAllMyBucketsResult")
public record ListAllMyBucketsResult(
    @JsonProperty("Owner")
    Owner owner,
    @JsonProperty("Buckets")
    Buckets buckets,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns")
    String xmlns
) {
  public ListAllMyBucketsResult {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public ListAllMyBucketsResult(Owner owner, Buckets buckets) {
    this(owner, buckets, null);
  }
}
