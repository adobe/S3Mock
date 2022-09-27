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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AccessControlPolicy.html">API Reference</a>.
 * This POJO uses JAX-B annotations instead of Jackson annotations because AWS decided to use
 * xsi:type annotations in the XML representation, which are not supported by Jackson.
 */
@XmlRootElement(name = "AccessControlPolicy")
@XmlAccessorType(XmlAccessType.FIELD)
public class AccessControlPolicy {
  @XmlElement(name = "Owner")
  private Owner owner;

  @XmlElement(name = "Grant")
  @XmlElementWrapper(name = "AccessControlList")
  private List<Grant> accessControlList;

  public AccessControlPolicy() {
    // Jackson needs the default constructor for deserialization.
  }

  public AccessControlPolicy(Owner owner, List<Grant> accessControlList) {
    this.owner = owner;
    this.accessControlList = accessControlList;
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
    AccessControlPolicy policy = (AccessControlPolicy) o;
    return Objects.equals(owner, policy.owner) && Objects.equals(
        accessControlList, policy.accessControlList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(owner, accessControlList);
  }
}
