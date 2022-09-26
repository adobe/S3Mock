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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html">API Reference</a>.
 */
@JsonRootName("CompletedPart")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record CompletedPart(@JsonProperty("PartNumber") Integer partNumber,
                            @JsonProperty("ETag") String etag,
                            @JsonProperty("ChecksumCRC32") String checksumCRC32,
                            @JsonProperty("ChecksumCRC32C") String checksumCRC32C,
                            @JsonProperty("ChecksumSHA1") String checksumSHA1,
                            @JsonProperty("ChecksumSHA256") String checksumSHA256) {

  public CompletedPart(@JsonProperty("PartNumber") Integer partNumber,
      @JsonProperty("ETag") String etag) {
    this(partNumber, normalizeEtag(etag), null, null, null, null);
  }
}
