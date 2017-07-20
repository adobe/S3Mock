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

import com.adobe.testing.s3mock.domain.Bucket;
import com.adobe.testing.s3mock.domain.Buckets;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamInclude;
import java.util.List;

/**
 * Represents a result of listing all Buckets.
 */
@XStreamAlias("ListAllMyBucketsResult") @XStreamInclude({Buckets.class})
public class ListAllMyBucketsResult {
  @XStreamAlias("Owner")
  private Owner owner;

  @XStreamAlias("Buckets")
  private Buckets buckets;

  /**
   * Constructs a new {@link ListAllMyBucketsResult}.
   */
  public ListAllMyBucketsResult() {
  }

  /**
   * Constructs a new {@link ListAllMyBucketsResult}.
   *
   * @param owner of which to list buckets
   * @param buckets list of buckets of the owner.
   */
  public ListAllMyBucketsResult(final Owner owner, final List<Bucket> buckets) {
    super();
    this.owner = owner;
    this.buckets = new Buckets();
    this.buckets.setBuckets(buckets);
  }

  /**
   * @return the buckets
   */
  public Buckets getBuckets() {
    return buckets;
  }

  /**
   * @return the owner
   */
  public Owner getOwner() {
    return owner;
  }

  /**
   * @param buckets the buckets to set
   */
  public void setBuckets(final Buckets buckets) {
    this.buckets = buckets;
  }

  /**
   * @param owner the owner to set
   */
  public void setOwner(final Owner owner) {
    this.owner = owner;
  }
}
