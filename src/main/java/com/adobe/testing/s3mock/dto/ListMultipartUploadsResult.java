/*
 *  Copyright 2017 Adobe.
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

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import java.util.List;

/**
 * List Multipart Uploads result according to the
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListMPUpload.html">S3 API
 * Reference</a>.
 */
@XStreamAlias("ListMultipartUploadsResult")
public class ListMultipartUploadsResult {
  @XStreamAlias("Bucket")
  private final String bucket;
  @XStreamAlias("KeyMarker")
  private final String keyMarker;
  @XStreamAlias("Delimiter")
  private final String delimiter;
  @XStreamAlias("Prefix")
  private final String prefix;
  @XStreamAlias("UploadIdMarker")
  private final String uploadIdMarker;
  @XStreamAlias("MaxUploads")
  private final int maxUploads;
  @XStreamAlias("IsTruncated")
  private final boolean isTruncated;
  @XStreamAlias("NextKeyMarker")
  private final String nextKeyMarker;
  @XStreamAlias("NextUploadIdMarker")
  private final String nextUploadIdMarker;
  @XStreamImplicit
  private final List<MultipartUpload> multipartUploads;
  @XStreamAlias("CommonPrefixes")
  private final java.util.List<String> commonPrefixes;

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
    this.commonPrefixes = commonPrefixes;
  }

  @Override
  public String toString() {
    return "ListMultipartUploadsResult{" +
        "bucket='" + bucket + '\'' +
        ", keyMarker='" + keyMarker + '\'' +
        ", delimiter='" + delimiter + '\'' +
        ", prefix='" + prefix + '\'' +
        ", uploadIdMarker='" + uploadIdMarker + '\'' +
        ", maxUploads=" + maxUploads +
        ", isTruncated=" + isTruncated +
        ", nextKeyMarker='" + nextKeyMarker + '\'' +
        ", nextUploadIdMarker='" + nextUploadIdMarker + '\'' +
        ", multipartUploads=" + multipartUploads +
        ", commonPrefixes=" + commonPrefixes +
        '}';
  }
}
