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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Date;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyPartResult.html">API Reference</a>.
 */
@JsonRootName("CopyPartResult")
public class CopyPartResult {

  @JsonProperty("LastModified")
  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private final Date lastModified;

  @JsonProperty("ETag")
  @JsonSerialize(using = EtagSerializer.class)
  private final String etag;

  public CopyPartResult(final Date lastModified, final String etag) {
    this.lastModified = lastModified;
    this.etag = etag;
  }

  public static CopyPartResult from(final Date date, final String etag) {
    return new CopyPartResult(date, etag);
  }

  public Date getLastModified() {
    return lastModified;
  }

  public String getEtag() {
    return etag;
  }
}
