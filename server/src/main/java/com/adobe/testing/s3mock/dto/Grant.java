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
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grant.html">API Reference</a>.
 */
@XmlRootElement(name = "Grant")
@XmlAccessorType(XmlAccessType.FIELD)
public class Grant {

  @XmlElement(name = "Grantee")
  private Grantee grantee;

  @XmlElement(name = "Permission")
  private Permission permission;

  public Grant() {
    // Jackson needs the default constructor for deserialization.
  }

  public Grant(Grantee grantee, Permission permission) {
    this.grantee = grantee;
    this.permission = permission;
  }

  public Grantee getGrantee() {
    return grantee;
  }

  public void setGrantee(Grantee grantee) {
    this.grantee = grantee;
  }

  public Permission getPermission() {
    return permission;
  }

  public void setPermission(Permission permission) {
    this.permission = permission;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Grant grant = (Grant) o;
    return Objects.equals(grantee, grant.grantee) && permission == grant.permission;
  }

  @Override
  public int hashCode() {
    return Objects.hash(grantee, permission);
  }

  public enum Permission {
    FULL_CONTROL("FULL_CONTROL"),
    WRITE("WRITE"),
    WRITE_ACP("WRITE_ACP"),
    READ("READ"),
    READ_ACP("READ_ACP");

    private final String value;

    @JsonCreator
    Permission(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }
}
