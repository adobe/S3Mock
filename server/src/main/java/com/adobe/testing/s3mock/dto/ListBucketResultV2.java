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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a result of listing objects that reside in a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
 */
@JsonRootName("ListBucketResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ListBucketResultV2 {

  @JsonProperty("Name")
  private String name;

  @JsonProperty("Prefix")
  private String prefix;

  @JsonProperty("MaxKeys")
  private int maxKeys;

  @JsonProperty("IsTruncated")
  private boolean isTruncated;

  @JsonProperty("Contents")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<S3Object> contents;

  @JsonProperty("CommonPrefixes")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<Prefix> commonPrefixes;

  @JsonProperty("ContinuationToken")
  private String continuationToken;

  @JsonProperty("KeyCount")
  private String keyCount;

  @JsonProperty("NextContinuationToken")
  private String nextContinuationToken;

  @JsonProperty("StartAfter")
  private String startAfter;

  @JsonProperty("EncodingType")
  private String encodingType;

  public ListBucketResultV2(final String name, final String prefix, final int maxKeys,
      final boolean isTruncated, final List<S3Object> contents,
      final Collection<String> commonPrefixes, final String continuationToken,
      final String keyCount, final String nextContinuationToken, final String startAfter,
      final String encodingType) {
    this.name = name;
    this.prefix = prefix;
    this.maxKeys = maxKeys;
    this.isTruncated = isTruncated;
    this.contents = new ArrayList<>();
    this.contents.addAll(contents);
    this.commonPrefixes = commonPrefixes.stream().map(Prefix::new).collect(Collectors.toList());
    this.continuationToken = continuationToken;
    this.keyCount = keyCount;
    this.nextContinuationToken = nextContinuationToken;
    this.startAfter = startAfter;
    this.encodingType = encodingType;
  }

  public String getName() {
    return name;
  }

  public String getPrefix() {
    return prefix;
  }

  public int getMaxKeys() {
    return maxKeys;
  }

  @JsonIgnore
  public boolean isTruncated() {
    return isTruncated;
  }

  public List<S3Object> getContents() {
    return contents;
  }

  public List<Prefix> getCommonPrefixes() {
    return commonPrefixes;
  }

  public String getContinuationToken() {
    return continuationToken;
  }

  public String getKeyCount() {
    return keyCount;
  }

  public String getNextContinuationToken() {
    return nextContinuationToken;
  }

  public String getStartAfter() {
    return startAfter;
  }

  public String getEncodingType() {
    return encodingType;
  }
}
