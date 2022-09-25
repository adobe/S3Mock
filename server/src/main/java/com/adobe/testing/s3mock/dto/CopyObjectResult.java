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

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObjectResult.html">API Reference</a>.
 */
@JsonRootName("CopyObjectResult")
public class CopyObjectResult {

  @JsonProperty("LastModified")
  private String lastModified;

  @JsonProperty("ETag")
  private String etag;

  /**
   * Constructs a new {@link CopyObjectResult}.
   *
   * @param lastModified last modification date of the copied file
   * @param etag the copied Files base64 MD5 Hash
   */
  public CopyObjectResult(final String lastModified, final String etag) {
    this.lastModified = lastModified;
    // make sure to store the etag correctly here, every usage depends on this...
    if (etag == null) {
      this.etag = etag;
    } else if (etag.startsWith("\"") && etag.endsWith("\"")) {
      this.etag = etag;
    } else {
      this.etag = String.format("\"%s\"", etag);
    }
  }
}
