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
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectLockRule.html">API Reference</a>.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ObjectLockRule {

  @JsonProperty("DefaultRetention")
  private  DefaultRetention defaultRetention;

  public ObjectLockRule() {
  }

  public ObjectLockRule(DefaultRetention defaultRetention) {
    this.defaultRetention = defaultRetention;
  }

  public DefaultRetention getDefaultRetention() {
    return defaultRetention;
  }

  public void setDefaultRetention(DefaultRetention defaultRetention) {
    this.defaultRetention = defaultRetention;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectLockRule that = (ObjectLockRule) o;
    return Objects.equals(defaultRetention, that.defaultRetention);
  }

  @Override
  public int hashCode() {
    return Objects.hash(defaultRetention);
  }
}
