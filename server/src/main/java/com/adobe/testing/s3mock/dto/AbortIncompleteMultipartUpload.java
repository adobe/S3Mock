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
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortIncompleteMultipartUpload.html">API Reference</a>.
 */
@JsonRootName("ListPartsResult")
public class AbortIncompleteMultipartUpload {

  @JsonProperty("DaysAfterInitiation")
  private Integer daysAfterInitiation;

  public AbortIncompleteMultipartUpload() {
  }

  public AbortIncompleteMultipartUpload(Integer daysAfterInitiation) {
    this.daysAfterInitiation = daysAfterInitiation;
  }

  public Integer getDaysAfterInitiation() {
    return daysAfterInitiation;
  }

  public void setDaysAfterInitiation(Integer daysAfterInitiation) {
    this.daysAfterInitiation = daysAfterInitiation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbortIncompleteMultipartUpload that = (AbortIncompleteMultipartUpload) o;
    return Objects.equals(daysAfterInitiation, that.daysAfterInitiation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(daysAfterInitiation);
  }
}
