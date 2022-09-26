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
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_S3Retention.html">API Reference</a>.
 * For unknown reasons, the timestamps in the Retention are serialized in Nanoseconds instead of
 * Milliseconds, like everywhere else.
 */
@JsonRootName("Retention")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Retention {

  @JsonProperty("Mode")
  private Mode mode;

  @JsonProperty("RetainUntilDate")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private Instant retainUntilDate;

  public Retention() {
    //Needed for Jackson
  }

  public Retention(Mode mode, Instant retainUntilDate) {
    this.mode = mode;
    this.retainUntilDate = retainUntilDate;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public Instant getRetainUntilDate() {
    return retainUntilDate;
  }

  public void setRetainUntilDate(Instant retainUntilDate) {
    this.retainUntilDate = retainUntilDate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Retention retention = (Retention) o;
    return mode == retention.mode && Objects.equals(retainUntilDate,
        retention.retainUntilDate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, retainUntilDate);
  }
}
