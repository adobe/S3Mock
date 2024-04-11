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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

/**
 * Represents a result of listing objects that reside in a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">API Reference</a>
 */
@JsonRootName("ListBucketResult")
public record ListBucketResult(
    @JsonProperty("Name")
    String name,
    @JsonProperty("Prefix")
    String prefix,
    @JsonProperty("Marker")
    String marker,
    @JsonProperty("MaxKeys")
    int maxKeys,
    @JsonProperty("IsTruncated")
    boolean isTruncated,
    @JsonProperty("EncodingType")
    String encodingType,
    @JsonProperty("NextMarker")
    String nextMarker,
    @JsonProperty("Contents")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<S3Object> contents,
    @JsonProperty("CommonPrefixes")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<Prefix> commonPrefixes,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns")
    String xmlns
) {

  public ListBucketResult {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public ListBucketResult(String name, String prefix, String marker, int maxKeys,
                          boolean isTruncated, String encodingType, String nextMarker,
                          List<S3Object> contents, List<Prefix> commonPrefixes) {
    this(name, prefix, marker, maxKeys, isTruncated, encodingType, nextMarker, contents,
        commonPrefixes, null);
  }
}
