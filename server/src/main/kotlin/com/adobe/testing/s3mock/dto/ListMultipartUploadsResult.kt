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
 * List Multipart Uploads result.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">API Reference</a>
 */
@S3Verified(year = 2025)
@JsonRootName("ListMultipartUploadsResult")
public record ListMultipartUploadsResult(
    @JsonProperty("Bucket") String bucket,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("CommonPrefixes") List<Prefix> commonPrefixes,
    @JsonProperty("Delimiter") String delimiter,
    @JsonProperty("EncodingType") String encodingType,
    @JsonProperty("IsTruncated") boolean isTruncated,
    @JsonProperty("KeyMarker") String keyMarker,
    @JsonProperty("MaxUploads") int maxUploads,
    @JsonProperty("NextKeyMarker") String nextKeyMarker,
    @JsonProperty("NextUploadIdMarker") String nextUploadIdMarker,
    @JsonProperty("Prefix") String prefix,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Upload") List<MultipartUpload> multipartUploads,
    @JsonProperty("UploadIdMarker") String uploadIdMarker,
    // workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {
  public ListMultipartUploadsResult {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public ListMultipartUploadsResult(String bucket, String keyMarker, String delimiter,
                                    String prefix, String uploadIdMarker, int maxUploads,
                                    boolean isTruncated, String nextKeyMarker,
                                    String nextUploadIdMarker,
                                    List<MultipartUpload> multipartUploads,
                                    List<Prefix> commonPrefixes,
                                    String encodingType) {
    this(bucket, commonPrefixes, delimiter, encodingType, isTruncated, keyMarker, maxUploads,
        nextKeyMarker, nextUploadIdMarker, prefix, multipartUploads, uploadIdMarker,
        null);
  }
}
