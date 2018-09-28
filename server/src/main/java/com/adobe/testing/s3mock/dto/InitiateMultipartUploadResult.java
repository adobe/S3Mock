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
 * Result to be returned after multipart upload initiation.
 */
@JsonRootName("InitiateMultipartUploadResult")
public class InitiateMultipartUploadResult {

  @JsonProperty("Bucket")
  private final String bucketName;

  @JsonProperty("Key")
  private final String fileName;

  @JsonProperty("UploadId")
  private final String uploadId;

  /**
   * Constructs a new {@link InitiateMultipartUploadResult}.
   *
   * @param bucketName the buckets name
   * @param fileName name/key
   * @param uploadId Id
   */
  public InitiateMultipartUploadResult(final String bucketName, final String fileName,
      final String uploadId) {
    this.bucketName = bucketName;
    this.fileName = fileName;
    this.uploadId = uploadId;
  }
}
