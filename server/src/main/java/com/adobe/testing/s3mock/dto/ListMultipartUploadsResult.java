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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;
import java.util.stream.Collectors;

/**
 * List Multipart Uploads result.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">API Reference</a>
 */
@JsonRootName("ListMultipartUploadsResult")
public class ListMultipartUploadsResult {

  @JsonProperty("Bucket")
  private final String bucket;
  @JsonProperty("KeyMarker")
  private final String keyMarker;
  @JsonProperty("Delimiter")
  private final String delimiter;
  @JsonProperty("Prefix")
  private final String prefix;
  @JsonProperty("UploadIdMarker")
  private final String uploadIdMarker;
  @JsonProperty("MaxUploads")
  private final int maxUploads;
  @JsonProperty("IsTruncated")
  private final boolean isTruncated;
  @JsonProperty("NextKeyMarker")
  private final String nextKeyMarker;
  @JsonProperty("NextUploadIdMarker")
  private final String nextUploadIdMarker;
  @JsonProperty("Upload")
  @JacksonXmlElementWrapper(useWrapping = false)
  private final List<MultipartUpload> multipartUploads;
  @JsonProperty("CommonPrefixes")
  @JacksonXmlElementWrapper(useWrapping = false)
  private final List<Prefix> commonPrefixes;

  public ListMultipartUploadsResult(final String bucket,
      final String keyMarker,
      final String delimiter,
      final String prefix,
      final String uploadIdMarker,
      final int maxUploads,
      final boolean isTruncated,
      final String nextKeyMarker,
      final String nextUploadIdMarker,
      final List<MultipartUpload> multipartUploads,
      final List<String> commonPrefixes) {
    this.bucket = bucket;
    this.keyMarker = keyMarker;
    this.delimiter = delimiter;
    this.prefix = prefix;
    this.uploadIdMarker = uploadIdMarker;
    this.maxUploads = maxUploads;
    this.isTruncated = isTruncated;
    this.nextKeyMarker = nextKeyMarker;
    this.nextUploadIdMarker = nextUploadIdMarker;
    this.multipartUploads = multipartUploads;
    this.commonPrefixes = commonPrefixes.stream().map(Prefix::new).collect(Collectors.toList());
  }
}
