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
import java.util.List;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRuleFilter.html">API Reference</a>.
 */
@JsonRootName("LifecycleRuleFilter")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LifecycleRuleFilter extends LifecycleRuleAndOperator {

  @JsonProperty("And")
  private LifecycleRuleAndOperator and;

  public LifecycleRuleFilter() {
  }

  public LifecycleRuleFilter(LifecycleRuleAndOperator and) {
    this.and = and;
  }

  public LifecycleRuleFilter(Long objectSizeGreaterThan, Long objectSizeLessThan, String prefix,
      List<Tag> tags, LifecycleRuleAndOperator and) {
    super(objectSizeGreaterThan, objectSizeLessThan, prefix, tags);
    this.and = and;
  }

  public LifecycleRuleAndOperator getAnd() {
    return and;
  }

  public void setAnd(LifecycleRuleAndOperator and) {
    this.and = and;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LifecycleRuleFilter that = (LifecycleRuleFilter) o;
    return Objects.equals(and, that.and);
  }

  @Override
  public int hashCode() {
    return Objects.hash(and);
  }
}
