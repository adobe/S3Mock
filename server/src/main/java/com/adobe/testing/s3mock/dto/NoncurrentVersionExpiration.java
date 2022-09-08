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
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_NoncurrentVersionExpiration.html">API Reference</a>.
 */
@JsonRootName("NoncurrentVersionExpiration")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class NoncurrentVersionExpiration {

  @JsonProperty("NewerNoncurrentVersions")
  private Integer newerNoncurrentVersions;
  @JsonProperty("NoncurrentDays")
  private Integer noncurrentDays;

  public NoncurrentVersionExpiration() {
  }

  public NoncurrentVersionExpiration(Integer newerNoncurrentVersions, Integer noncurrentDays) {
    this.newerNoncurrentVersions = newerNoncurrentVersions;
    this.noncurrentDays = noncurrentDays;
  }

  public Integer getNewerNoncurrentVersions() {
    return newerNoncurrentVersions;
  }

  public void setNewerNoncurrentVersions(Integer newerNoncurrentVersions) {
    this.newerNoncurrentVersions = newerNoncurrentVersions;
  }

  public Integer getNoncurrentDays() {
    return noncurrentDays;
  }

  public void setNoncurrentDays(Integer noncurrentDays) {
    this.noncurrentDays = noncurrentDays;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NoncurrentVersionExpiration that = (NoncurrentVersionExpiration) o;
    return Objects.equals(newerNoncurrentVersions, that.newerNoncurrentVersions)
        && Objects.equals(noncurrentDays, that.noncurrentDays);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newerNoncurrentVersions, noncurrentDays);
  }
}
