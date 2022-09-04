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
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectLockConfiguration.html">API Reference</a>.
 */
@JsonRootName("ObjectLockConfiguration")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ObjectLockConfiguration {

  @JsonProperty("ObjectLockEnabled")
  private ObjectLockEnabled objectLockEnabled;

  @JsonProperty("Rule")
  private ObjectLockRule objectLockRule;

  public ObjectLockConfiguration() {
  }

  public ObjectLockConfiguration(ObjectLockEnabled objectLockEnabled,
      ObjectLockRule objectLockRule) {
    this.objectLockEnabled = objectLockEnabled;
    this.objectLockRule = objectLockRule;
  }

  public ObjectLockEnabled getObjectLockEnabled() {
    return objectLockEnabled;
  }

  public void setObjectLockEnabled(ObjectLockEnabled objectLockEnabled) {
    this.objectLockEnabled = objectLockEnabled;
  }

  public ObjectLockRule getObjectLockRule() {
    return objectLockRule;
  }

  public void setObjectLockRule(ObjectLockRule objectLockRule) {
    this.objectLockRule = objectLockRule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectLockConfiguration that = (ObjectLockConfiguration) o;
    return objectLockEnabled == that.objectLockEnabled && Objects.equals(objectLockRule,
        that.objectLockRule);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectLockEnabled, objectLockRule);
  }
}
