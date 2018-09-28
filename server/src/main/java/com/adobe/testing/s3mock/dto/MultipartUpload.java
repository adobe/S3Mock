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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.Objects;

/**
 * Container for elements related to a particular multipart upload, according to the
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListMPUpload.html">S3 API
 * Reference</a>.
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
  private final String storageClass = "STANDARD";
  @JsonProperty("Initiated")
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private final Date initiated;

  /**
   * Create a new MultipartUpload with the given parameters.
   *
   * @param key Object Key.
   * @param uploadId UploadId.
   * @param owner The {@link Owner}.
   * @param initiator The initiator.
   * @param initiated Date when initiated.
   */
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

  public String getStorageClass() {
    return storageClass;
  }

  public Date getInitiated() {
    return initiated;
  }

  @Override
  public String toString() {
    return "MultipartUpload{"
        + "key='" + key + '\''
        + ", uploadId='" + uploadId + '\''
        + ", owner=" + owner
        + ", initiator=" + initiator
        + ", storageClass='" + storageClass + '\''
        + ", initiated=" + initiated
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MultipartUpload that = (MultipartUpload) o;
    return Objects.equals(key, that.key)
        && Objects.equals(uploadId, that.uploadId)
        && Objects.equals(owner, that.owner)
        && Objects.equals(initiator, that.initiator)
        && Objects.equals(storageClass, that.storageClass)
        && Objects.equals(initiated, that.initiated);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, uploadId, owner, initiator, storageClass, initiated);
  }
}
