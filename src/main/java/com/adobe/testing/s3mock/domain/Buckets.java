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

package com.adobe.testing.s3mock.domain;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import java.util.List;

/**
 * DTO representing a list of buckets.
 */
@XStreamAlias("Buckets")
public class Buckets {
  @XStreamImplicit private List<Bucket> buckets;

  /**
   * @return the list of buckets
   */
  public List<Bucket> getBuckets() {
    return buckets;
  }

  /**
   * @param buckets the list of buckets to set
   */
  public void setBuckets(final List<Bucket> buckets) {
    this.buckets = buckets;
  }
}
