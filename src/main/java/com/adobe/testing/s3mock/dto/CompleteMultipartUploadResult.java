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

/**
 * Result to be returned when completing a multipart request.
 */
@XStreamAlias("CompleteMultipartUploadResult")
public class CompleteMultipartUploadResult {
  @XStreamAlias("Location")
  private final String location;

  @XStreamAlias("Bucket")
  private final String bucket;

  @XStreamAlias("Key")
  private final String key;

  @XStreamAlias("ETag")
  private final String eTag;

  /**
   * Constructs a new {@link CompleteMultipartUploadResult}.
   * @param location s3 url.
   * @param bucket bucket name
   * @param key filename
   * @param eTag of the overall file.
   *
   */
  public CompleteMultipartUploadResult(final String location, final String bucket, final String key, final String eTag) {
    this.location = location;
    this.bucket = bucket;
    this.key = key;
    this.eTag = eTag;
  }
}
