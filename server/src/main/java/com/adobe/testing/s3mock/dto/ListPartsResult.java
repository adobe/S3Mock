/*
 *  Copyright 2017-2023 Adobe.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

/**
 * List-Parts result with some hard-coded values as this is sufficient for now.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">API Reference</a>
 */
@JsonRootName("ListPartsResult")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ListPartsResult(
    @JsonProperty("Bucket")
    String bucket,
    @JsonProperty("Key")
    String key,
    @JsonProperty("UploadId")
    String uploadId,
    @JsonProperty("PartNumberMarker")
    String partNumberMarker,
    @JsonProperty("NextPartNumberMarker")
    String nextPartNumberMarker,
    @JsonProperty("IsTruncated")
    boolean truncated,
    @JsonProperty("StorageClass")
    StorageClass storageClass,
    @JsonProperty("Part")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<Part> parts,
    @JsonProperty("Owner")
    Owner owner,
    @JsonProperty("Initiator")
    Owner initiator,
    @JsonProperty("ChecksumAlgorithm")
    ChecksumAlgorithm checksumAlgorithm
) {

  public ListPartsResult {
    partNumberMarker = partNumberMarker == null ? "0" : partNumberMarker;
    nextPartNumberMarker = nextPartNumberMarker == null ? "1" : nextPartNumberMarker;
    storageClass = storageClass == null ? StorageClass.STANDARD : storageClass;
  }

  public ListPartsResult(String bucketName,
      String key,
      String uploadId,
      List<Part> parts) {
    this(bucketName, key, uploadId, null, null, false, null, parts,
        null, null, null);
  }
}
