/*
 *  Copyright 2017-2025 Adobe.
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

import com.adobe.testing.S3Verified;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AccessControlPolicy.html">API Reference</a>.
 * This class is a POJO instead of a record because jackson-databind-xml as of now does not support
 * record classes with @JacksonXmlElementWrapper:
 * https://github.com/FasterXML/jackson-dataformat-xml/issues/517
 */
@S3Verified(year = 2025)
@JsonRootName("AccessControlPolicy")
public class AccessControlPolicy {
  @JsonProperty("Grant")
  @JacksonXmlElementWrapper(localName = "AccessControlList") List<Grant> accessControlList;
  @JsonProperty("Owner") Owner owner;
  // workaround for adding xmlns attribute to root element only.
  @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns;

  public AccessControlPolicy() {
    // needed by Jackson
  }

  public AccessControlPolicy(Owner owner, List<Grant> accessControlList, String xmlns) {
    this.owner = owner;
    this.accessControlList = accessControlList;
    this.xmlns = xmlns;
  }

  @JsonCreator(mode = JsonCreator.Mode.DISABLED)
  public AccessControlPolicy(Owner owner, List<Grant> accessControlList) {
    this(owner, accessControlList, "http://s3.amazonaws.com/doc/2006-03-01/");
  }

  public Owner getOwner() {
    return owner;
  }

  public void setOwner(Owner owner) {
    this.owner = owner;
  }

  public List<Grant> getAccessControlList() {
    return accessControlList;
  }

  public void setAccessControlList(List<Grant> accessControlList) {
    this.accessControlList = accessControlList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccessControlPolicy that = (AccessControlPolicy) o;
    return Objects.equals(owner, that.owner)
        && Objects.equals(accessControlList, that.accessControlList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(owner, accessControlList);
  }

  @Override
  public String toString() {
    return "AccessControlPolicy{"
        + "owner=" + owner
        + ", accessControlList=" + accessControlList
        + ", xmlns='" + xmlns + '\''
        + '}';
  }
}
