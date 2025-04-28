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

package com.adobe.testing.s3mock.dto;

import com.adobe.testing.S3Verified;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

/**
 * Represents a result of listing objects that reside in a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
 */
@S3Verified(year = 2025)
@JsonRootName("ListBucketResult")
public record ListBucketResultV2(
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("CommonPrefixes") List<Prefix> commonPrefixes,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Contents") List<S3Object> contents,
    @JsonProperty("ContinuationToken") String continuationToken,
    @JsonProperty("Delimiter") String delimiter,
    @JsonProperty("EncodingType") String encodingType,
    @JsonProperty("IsTruncated") boolean isTruncated,
    @JsonProperty("KeyCount") String keyCount,
    @JsonProperty("MaxKeys") int maxKeys,
    @JsonProperty("Name") String name,
    @JsonProperty("NextContinuationToken") String nextContinuationToken,
    @JsonProperty("Prefix") String prefix,
    @JsonProperty("StartAfter") String startAfter,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {
  public ListBucketResultV2 {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public ListBucketResultV2(List<Prefix> commonPrefixes, List<S3Object> contents, String continuationToken,
                            String delimiter, String encodingType, boolean isTruncated, int maxKeys, String name,
                            String nextContinuationToken, String prefix, String keyCount, String startAfter) {
    this(commonPrefixes, contents, continuationToken, delimiter, encodingType, isTruncated, keyCount, maxKeys,
        name, nextContinuationToken, prefix, startAfter, null);
  }
}
