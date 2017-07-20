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
 * Result to be returned after multipart upload initiation.
 */
@XStreamAlias("InitiateMultipartUploadResult")
public class InitiateMultipartUploadResult {
  @XStreamAlias("Bucketname")
  private final String bucketName;

  @XStreamAlias("Key")
  private final String fileName;

  @XStreamAlias("UploadId")
  private final String uploadId;

  /**
   * Constructs a new {@link InitiateMultipartUploadResult}.
   *
   * @param bucketName the buckets name
   * @param fileName name/key
   * @param uploadId Id
   *
   */
  public InitiateMultipartUploadResult(final String bucketName, final String fileName,
      final String uploadId) {
    this.bucketName = bucketName;
    this.fileName = fileName;
    this.uploadId = uploadId;
  }
}
