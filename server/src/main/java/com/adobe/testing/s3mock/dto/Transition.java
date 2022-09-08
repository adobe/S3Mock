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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Transition.html">API Reference</a>.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html">API Reference</a>.
 */
@JsonRootName("Transition")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Transition {

  @JsonProperty("Date")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant date;
  @JsonProperty("Days")
  private Integer days;
  @JsonProperty("StorageClass")
  private StorageClass storageClass;

  public Transition() {
  }

  public Transition(Instant date, Integer days, StorageClass storageClass) {
    this.date = date;
    this.days = days;
    this.storageClass = storageClass;
  }

  public Instant getDate() {
    return date;
  }

  public void setDate(Instant date) {
    this.date = date;
  }

  public Integer getDays() {
    return days;
  }

  public void setDays(Integer days) {
    this.days = days;
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
    Transition that = (Transition) o;
    return Objects.equals(date, that.date) && Objects.equals(days, that.days)
        && storageClass == that.storageClass;
  }

  @Override
  public int hashCode() {
    return Objects.hash(date, days, storageClass);
  }
}
