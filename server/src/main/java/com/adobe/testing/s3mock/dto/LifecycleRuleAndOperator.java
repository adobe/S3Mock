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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRuleAndOperator.html">API Reference</a>.
 */
@JsonRootName("LifecycleRuleAndOperator")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LifecycleRuleAndOperator {

  @JsonProperty("ObjectSizeGreaterThan")
  private Long objectSizeGreaterThan;
  @JsonProperty("ObjectSizeLessThan")
  private Long objectSizeLessThan;
  @JsonProperty("Prefix")
  private String prefix;
  @JsonProperty("Tags")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<Tag> tags;

  public LifecycleRuleAndOperator() {
  }

  public LifecycleRuleAndOperator(Long objectSizeGreaterThan, Long objectSizeLessThan,
      String prefix,
      List<Tag> tags) {
    this.objectSizeGreaterThan = objectSizeGreaterThan;
    this.objectSizeLessThan = objectSizeLessThan;
    this.prefix = prefix;
    this.tags = tags;
  }

  public Long getObjectSizeGreaterThan() {
    return objectSizeGreaterThan;
  }

  public void setObjectSizeGreaterThan(Long objectSizeGreaterThan) {
    this.objectSizeGreaterThan = objectSizeGreaterThan;
  }

  public Long getObjectSizeLessThan() {
    return objectSizeLessThan;
  }

  public void setObjectSizeLessThan(Long objectSizeLessThan) {
    this.objectSizeLessThan = objectSizeLessThan;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public List<Tag> getTags() {
    return tags;
  }

  public void setTags(List<Tag> tags) {
    this.tags = tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LifecycleRuleAndOperator that = (LifecycleRuleAndOperator) o;
    return Objects.equals(objectSizeGreaterThan, that.objectSizeGreaterThan)
        && Objects.equals(objectSizeLessThan, that.objectSizeLessThan)
        && Objects.equals(prefix, that.prefix) && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectSizeGreaterThan, objectSizeLessThan, prefix, tags);
  }
}
