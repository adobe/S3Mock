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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;

/**
 * Container for elements related to a particular multipart upload.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_MultipartUpload.html">API Reference</a>
 */
public class MultipartUpload {

  @JsonProperty("Key")
  private final String key;
  @JsonProperty("UploadId")
  private final String uploadId;
  @JsonProperty("Owner")
  private final Owner owner;
  @JsonProperty("Initiator")
  private final Owner initiator;
  @JsonProperty("StorageClass")
  private final StorageClass storageClass = StorageClass.STANDARD;
  @JsonProperty("Initiated")
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private final Date initiated;

  public MultipartUpload(final String key, final String uploadId, final Owner owner,
      final Owner initiator, final Date initiated) {
    this.key = key;
    this.uploadId = uploadId;
    this.owner = owner;
    this.initiator = initiator;
    this.initiated = initiated;
  }

  public String getKey() {
    return key;
  }

  public String getUploadId() {
    return uploadId;
  }

  public Owner getOwner() {
    return owner;
  }

  public Owner getInitiator() {
    return initiator;
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public Date getInitiated() {
    return initiated;
  }

}
