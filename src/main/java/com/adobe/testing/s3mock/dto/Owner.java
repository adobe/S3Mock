/*
 *  Copyright 2017 Adobe.
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

import com.thoughtworks.xstream.annotations.XStreamAlias;
import javax.xml.bind.annotation.XmlElement;

/**
 * Owner of an Bucket.
 */
@XStreamAlias("Owner")
public class Owner {
  @XStreamAlias("ID")
  private long id;

  @XStreamAlias("DisplayName")
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
    super();
    this.id = id;
    this.displayName = displayName;
  }

  /**
   * @return the displayName
   */
  @XmlElement(name = "DisplayName")
  public String getDisplayName() {
    return displayName;
  }

  /**
   * @return the id
   */
  @XmlElement(name = "ID")
  public long getId() {
    return id;
  }

  /**
   * @param displayName the displayName to set
   */
  public void setDisplayName(final String displayName) {
    this.displayName = displayName;
  }

  /**
   * @param id the id to set
   */
  public void setId(final long id) {
    this.id = id;
  }
}
