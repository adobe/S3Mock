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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html">API Reference</a>.
 */
@JsonRootName("LifecycleRule")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LifecycleRule {

  @JsonProperty("AbortIncompleteMultipartUpload")
  private AbortIncompleteMultipartUpload abortIncompleteMultipartUpload;
  @JsonProperty("Expiration")
  private LifecycleExpiration expiration;
  @JsonProperty("Filter")
  private LifecycleRuleFilter filter;
  @JsonProperty("ID")
  private String id;
  @JsonProperty("NoncurrentVersionExpiration")
  @JacksonXmlElementWrapper(useWrapping = false)
  private NoncurrentVersionExpiration noncurrentVersionExpiration;
  @JsonProperty("NoncurrentVersionTransition")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<NoncurrentVersionTransition> noncurrentVersionTransitions;
  @JsonProperty("Status")
  private Status status;

  @JsonProperty("Transition")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<Transition> transitions;

  public LifecycleRule() {
  }

  public LifecycleRule(AbortIncompleteMultipartUpload abortIncompleteMultipartUpload,
      LifecycleExpiration expiration, LifecycleRuleFilter filter, String id,
      NoncurrentVersionExpiration noncurrentVersionExpiration,
      List<NoncurrentVersionTransition> noncurrentVersionTransitions, Status status,
      List<Transition> transitions) {
    this.abortIncompleteMultipartUpload = abortIncompleteMultipartUpload;
    this.expiration = expiration;
    this.filter = filter;
    this.id = id;
    this.noncurrentVersionExpiration = noncurrentVersionExpiration;
    this.noncurrentVersionTransitions = noncurrentVersionTransitions;
    this.status = status;
    this.transitions = transitions;
  }

  public AbortIncompleteMultipartUpload getAbortIncompleteMultipartUpload() {
    return abortIncompleteMultipartUpload;
  }

  public void setAbortIncompleteMultipartUpload(
      AbortIncompleteMultipartUpload abortIncompleteMultipartUpload) {
    this.abortIncompleteMultipartUpload = abortIncompleteMultipartUpload;
  }

  public LifecycleExpiration getExpiration() {
    return expiration;
  }

  public void setExpiration(LifecycleExpiration expiration) {
    this.expiration = expiration;
  }

  public LifecycleRuleFilter getFilter() {
    return filter;
  }

  public void setFilter(LifecycleRuleFilter filter) {
    this.filter = filter;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public NoncurrentVersionExpiration getNoncurrentVersionExpiration() {
    return noncurrentVersionExpiration;
  }

  public void setNoncurrentVersionExpiration(
      NoncurrentVersionExpiration noncurrentVersionExpiration) {
    this.noncurrentVersionExpiration = noncurrentVersionExpiration;
  }

  public List<NoncurrentVersionTransition> getNoncurrentVersionTransitions() {
    return noncurrentVersionTransitions;
  }

  public void setNoncurrentVersionTransitions(
      List<NoncurrentVersionTransition> noncurrentVersionTransitions) {
    this.noncurrentVersionTransitions = noncurrentVersionTransitions;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public List<Transition> getTransitions() {
    return transitions;
  }

  public void setTransitions(List<Transition> transitions) {
    this.transitions = transitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LifecycleRule that = (LifecycleRule) o;
    return
        Objects.equals(abortIncompleteMultipartUpload, that.abortIncompleteMultipartUpload)
            && Objects.equals(expiration, that.expiration) && Objects.equals(
            filter, that.filter) && Objects.equals(id, that.id) && Objects.equals(
            noncurrentVersionExpiration, that.noncurrentVersionExpiration)
            && Objects.equals(noncurrentVersionTransitions,
            that.noncurrentVersionTransitions) && status == that.status && Objects.equals(
            transitions, that.transitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(abortIncompleteMultipartUpload, expiration, filter, id,
        noncurrentVersionExpiration, noncurrentVersionTransitions, status, transitions);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html">API Reference</a>.
   */
  public enum Status {
    ENABLED("Enabled"),
    DISABLED("Disabled");

    private final String value;

    @JsonCreator
    Status(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }
}
