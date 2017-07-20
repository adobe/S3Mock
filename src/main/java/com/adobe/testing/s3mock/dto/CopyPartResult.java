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

import com.amazonaws.util.DateUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import java.util.Date;

@XStreamAlias("CopyPartResult")
public class CopyPartResult {
  @XStreamAlias("LastModified")
  private final String lastModified;

  @XStreamAlias("ETag")
  private final String etag;

  public CopyPartResult(final String lastModified, final String etag) {
    this.lastModified = lastModified;
    this.etag = etag;
  }

  public static CopyPartResult from(final Date date, final String etag) {
    return new CopyPartResult(DateUtils.formatISO8601Date(new Date()), etag);
  }
}
