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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.Objects;

/**
 * This Class extends {@link CompletedPart} to reduce code duplication.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Part.html">API Reference</a>
 */
public class Part extends CompletedPart {

  @JsonProperty("LastModified")
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private Date lastModified;

  @JsonProperty("Size")
  private Long size;

  public Part(@JsonProperty("PartNumber") Integer partNumber,
      @JsonProperty("ETag") String etag,
      @JsonProperty("LastModified")
      @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC") Date lastModified,
      @JsonProperty("Size") Long size) {
    super(partNumber, etag);
    this.lastModified = lastModified;
    this.size = size;
  }

  public Date getLastModified() {
    return lastModified;
  }

  public void setLastModified(final Date lastModified) {
    this.lastModified = lastModified;
  }

  public Long getSize() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Part part = (Part) o;
    return Objects.equals(partNumber, part.partNumber) && Objects.equals(
        lastModified, part.lastModified) && Objects.equals(etag, part.etag)
        && Objects.equals(size, part.size);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partNumber, lastModified, etag, size);
  }
}
