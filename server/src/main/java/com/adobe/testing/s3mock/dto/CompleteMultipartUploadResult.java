/*
 *  Copyright 2017-2018 Adobe.
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
 * Result to be returned when completing a multipart request.
 */
@JsonRootName("CompleteMultipartUploadResult")
public class CompleteMultipartUploadResult {

  @JsonProperty("Location")
  private final String location;

  @JsonProperty("Bucket")
  private final String bucket;

  @JsonProperty("Key")
  private final String key;

  @JsonProperty("ETag")
  private final String etag;

  /**
   * Constructs a new {@link CompleteMultipartUploadResult}.
   *
   * @param location s3 url.
   * @param bucket bucket name
   * @param key filename
   * @param etag of the overall file.
   */
  public CompleteMultipartUploadResult(final String location, final String bucket, final String key,
      final String etag) {
    this.location = location;
    this.bucket = bucket;
    this.key = key;
    this.etag = etag;
  }
}
