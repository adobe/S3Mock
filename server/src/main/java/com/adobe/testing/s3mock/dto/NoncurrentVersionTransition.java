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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_NoncurrentVersionTransition.html">API Reference</a>.
 */
@JsonRootName("NoncurrentVersionTransition")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class NoncurrentVersionTransition extends NoncurrentVersionExpiration {

  @JsonProperty("StorageClass")
  private StorageClass storageClass;

  public NoncurrentVersionTransition() {
  }

  public NoncurrentVersionTransition(StorageClass storageClass) {
    this.storageClass = storageClass;
  }

  public NoncurrentVersionTransition(Integer newerNoncurrentVersions, Integer noncurrentDays,
      StorageClass storageClass) {
    super(newerNoncurrentVersions, noncurrentDays);
    this.storageClass = storageClass;
  }

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(StorageClass storageClass) {
    this.storageClass = storageClass;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NoncurrentVersionTransition that = (NoncurrentVersionTransition) o;
    return storageClass == that.storageClass;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageClass);
  }
}
