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

/**
 * DTO as representation of a succeeded copy process.
 */
@XStreamAlias("CopyObjectResult")
public class CopyObjectResult {
  @XStreamAlias("LastModified")
  private String lastModified;

  @XStreamAlias("ETag")
  private String eTag;

  /**
   * Constructs a new {@link CopyObjectResult}.
   *
   * @param lastModified last modification date of the copied file
   * @param eTag the copied Files base64 MD5 Hash
   */
  public CopyObjectResult(final String lastModified, final String eTag) {
    this.lastModified = lastModified;
    this.eTag = eTag;
  }

  /**
   * @return the lastModified
   */
  public String getLastModified() {
    return lastModified;
  }

  /**
   * @param lastModified the lastModified to set
   */
  public void setLastModified(final String lastModified) {
    this.lastModified = lastModified;
  }

  /**
   * @return the eTag
   */
  public String geteTag() {
    return eTag;
  }

  /**
   * @param eTag the eTag to set
   */
  public void seteTag(final String eTag) {
    this.eTag = eTag;
  }
}
