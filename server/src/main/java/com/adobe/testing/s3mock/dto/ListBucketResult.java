/*
 *  Copyright 2017-2021 Adobe.
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

import com.adobe.testing.s3mock.domain.BucketContents;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a result of listing objects that reside in a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">S3 API
 * Reference</a>.
 */
@JsonRootName("ListBucketResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ListBucketResult implements Serializable {

  @JsonProperty("Name")
  private String name;

  @JsonProperty("Prefix")
  private String prefix;

  @JsonProperty("Marker")
  private String marker;

  @JsonProperty("MaxKeys")
  private int maxKeys;

  @JsonProperty("IsTruncated")
  private boolean isTruncated;

  @JsonProperty("EncodingType")
  private String encodingType;

  @JsonProperty("NextMarker")
  private String nextMarker;

  @JsonProperty("Contents")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<BucketContents> contents;

  @JsonProperty("CommonPrefixes")
  private CommonPrefixes commonPrefixes;

  /**
   * Constructs a new {@link ListBucketResult}.
   *
   * @param name {@link String}
   * @param prefix {@link String}
   * @param marker {@link String}
   * @param maxKeys {@link String}
   * @param isTruncated {@link Boolean}
   * @param encodingType {@link String}
   * @param nextMarker {@link String}
   * @param contents {@link List}
   * @param commonPrefixes {@link String}
   */
  public ListBucketResult(final String name,
      final String prefix,
      final String marker,
      final int maxKeys,
      final boolean isTruncated,
      final String encodingType,
      final String nextMarker,
      final List<BucketContents> contents,
      final Collection<String> commonPrefixes) {
    this.name = name;
    this.prefix = prefix;
    this.marker = marker;
    this.maxKeys = maxKeys;
    this.isTruncated = isTruncated;
    this.encodingType = encodingType;
    this.nextMarker = nextMarker;
    this.contents = new ArrayList<>();
    this.contents.addAll(contents);
    this.commonPrefixes = commonPrefixes == null || commonPrefixes.isEmpty() ? null :
        new CommonPrefixes(commonPrefixes);
  }
}
