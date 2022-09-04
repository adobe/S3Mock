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
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DefaultRetention.html">API Reference</a>.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DefaultRetention {

  @JsonProperty("Days")
  private Integer days;

  @JsonProperty("Years")
  private Integer years;

  @JsonProperty("Mode")
  private Mode mode;

  public DefaultRetention() {
  }

  public DefaultRetention(Integer days, Integer years, Mode mode) {
    //TODO: setting days & years not allowed!
    this.days = days;
    this.years = years;
    this.mode = mode;
  }

  public Integer getDays() {
    return days;
  }

  public void setDays(Integer days) {
    this.days = days;
  }

  public Integer getYears() {
    return years;
  }

  public void setYears(Integer years) {
    this.years = years;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DefaultRetention retention = (DefaultRetention) o;
    return Objects.equals(days, retention.days) && Objects.equals(years,
        retention.years) && mode == retention.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(days, years, mode);
  }
}
