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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * Owner of a Bucket.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Owner.html">API Reference</a>
 */
@XmlRootElement(name = "Owner")
@XmlAccessorType(XmlAccessType.FIELD)
public class Owner {

  /**
   * Default owner in S3Mock until support for ownership is implemented.
   */
  public static final Owner DEFAULT_OWNER =
      new Owner("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
          "s3-mock-file-store");

  @XmlElement(name = "ID")
  @JsonProperty("ID")
  private String id;
  @XmlElement(name = "DisplayName")
  @JsonProperty("DisplayName")
  private String displayName;

  public Owner() {
    // Jackson needs the default constructor for deserialization.
  }

  public Owner(String id, String displayName) {
    this.id = id;
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getId() {
    return id;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Owner owner = (Owner) o;
    return Objects.equals(id, owner.id) && Objects.equals(displayName,
        owner.displayName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, displayName);
  }
}
