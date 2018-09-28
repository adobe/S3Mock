/*
 *  Copyright 2017-2018 Adobe.
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
import com.fasterxml.jackson.annotation.JsonRootName;
import javax.xml.bind.annotation.XmlElement;

/**
 * Owner of a Bucket.
 */
@JsonRootName("Owner")
public class Owner {

  @JsonProperty("ID")
  private long id;

  @JsonProperty("DisplayName")
  private String displayName;

  /**
   * Constructs a new {@link Owner}.
   */
  public Owner() {
  }

  /**
   * Constructs a new {@link Owner}.
   *
   * @param id of the owner.
   * @param displayName name of ther owner.
   */
  public Owner(final long id, final String displayName) {
    this.id = id;
    this.displayName = displayName;
  }

  @XmlElement(name = "DisplayName")
  public String getDisplayName() {
    return displayName;
  }

  @XmlElement(name = "ID")
  public long getId() {
    return id;
  }

  public void setDisplayName(final String displayName) {
    this.displayName = displayName;
  }

  public void setId(final long id) {
    this.id = id;
  }
}
