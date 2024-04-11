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
 * List Multipart Uploads result.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">API Reference</a>
 */
@JsonRootName("ListMultipartUploadsResult")
public record ListMultipartUploadsResult(
    @JsonProperty("Bucket")
    String bucket,
    @JsonProperty("KeyMarker")
    String keyMarker,
    @JsonProperty("Delimiter")
    String delimiter,
    @JsonProperty("Prefix")
    String prefix,
    @JsonProperty("UploadIdMarker")
    String uploadIdMarker,
    @JsonProperty("MaxUploads")
    int maxUploads,
    @JsonProperty("IsTruncated")
    boolean isTruncated,
    @JsonProperty("NextKeyMarker")
    String nextKeyMarker,
    @JsonProperty("NextUploadIdMarker")
    String nextUploadIdMarker,
    @JsonProperty("Upload")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<MultipartUpload> multipartUploads,
    @JsonProperty("CommonPrefixes")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<Prefix> commonPrefixes,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns")
    String xmlns
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
                                    List<Prefix> commonPrefixes) {
    this(bucket, keyMarker, delimiter, prefix, uploadIdMarker, maxUploads, isTruncated,
        nextKeyMarker, nextUploadIdMarker, multipartUploads, commonPrefixes, null);
  }
}
