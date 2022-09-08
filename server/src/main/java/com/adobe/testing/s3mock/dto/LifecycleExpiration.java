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
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleExpiration.html">API Reference</a>.
 */
@JsonRootName("LifecycleExpiration")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LifecycleExpiration {

  @JsonProperty("Date")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant date;

  @JsonProperty("Days")
  private Integer days;

  @JsonProperty("ExpiredObjectDeleteMarker")
  private Boolean expiredObjectDeleteMarker;

  public LifecycleExpiration() {
  }

  public LifecycleExpiration(Instant date, Integer days, Boolean expiredObjectDeleteMarker) {
    this.date = date;
    this.days = days;
    this.expiredObjectDeleteMarker = expiredObjectDeleteMarker;
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

  public Boolean getExpiredObjectDeleteMarker() {
    return expiredObjectDeleteMarker;
  }

  public void setExpiredObjectDeleteMarker(Boolean expiredObjectDeleteMarker) {
    this.expiredObjectDeleteMarker = expiredObjectDeleteMarker;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LifecycleExpiration that = (LifecycleExpiration) o;
    return Objects.equals(date, that.date) && Objects.equals(days, that.days)
        && Objects.equals(expiredObjectDeleteMarker, that.expiredObjectDeleteMarker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(date, days, expiredObjectDeleteMarker);
  }
}
