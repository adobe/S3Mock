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
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;
import java.net.URI;
import java.util.Objects;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grantee.html">API Reference</a>.
 */
@XmlRootElement(name = "Grantee")
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class Grantee extends Owner {

  @XmlElement(name = "EmailAddress")
  private String emailAddress;
  @XmlElement(name = "URI")
  private URI uri;

  public Grantee() {
    // Jackson needs the default constructor for deserialization.
  }

  public Grantee(String id, String displayName, String emailAddress, URI uri) {
    super(id, displayName);
    this.emailAddress = emailAddress;
    this.uri = uri;
  }

  public static Grantee from(Owner owner) {
    return new CanonicalUser(owner.getId(), owner.getDisplayName(), null, null);
  }

  public String getEmailAddress() {
    return emailAddress;
  }

  public void setEmailAddress(String emailAddress) {
    this.emailAddress = emailAddress;
  }

  public URI getUri() {
    return uri;
  }

  public void setUri(URI uri) {
    this.uri = uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Grantee grantee = (Grantee) o;
    return Objects.equals(emailAddress, grantee.emailAddress) && Objects.equals(
        uri, grantee.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), emailAddress, uri);
  }

  @XmlType(name = "CanonicalUser")
  public static class CanonicalUser extends Grantee {
    public CanonicalUser() {
    }

    public CanonicalUser(String id, String displayName, String emailAddress, URI uri) {
      super(id, displayName, emailAddress, uri);
    }
  }

  @XmlType(name = "Group")
  public static class Group extends Grantee {
    public Group() {
    }

    public Group(String id, String displayName, String emailAddress, URI uri) {
      super(id, displayName, emailAddress, uri);
    }
  }

  @XmlType(name = "AmazonCustomerByEmail")
  public static class AmazonCustomerByEmail extends Grantee {
    public AmazonCustomerByEmail() {
    }

    public AmazonCustomerByEmail(String id, String displayName, String emailAddress,
        URI uri) {
      super(id, displayName, emailAddress, uri);
    }
  }
}
