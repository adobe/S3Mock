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
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * Contents are the XMLElements of ListBucketResult see http://docs.aws.amazon
 * .com/AmazonS3/latest/API/RESTBucketGET.html
 */
@JsonRootName("Contents")
public class BucketContents {

  @JsonProperty("Key")
  private String key;

  @JsonProperty("LastModified")
  private String lastModified;

  @JsonProperty("ETag")
  private String etag;

  @JsonProperty("Size")
  private String size;

  @JsonProperty("StorageClass")
  private String storageClass;

  @JsonProperty("Owner")
  private Owner owner;

  /**
   * Constructs a new {@link BucketContents}.
   */
  public BucketContents() {
    // empty here
  }

  /**
   * Constructs a new {@link BucketContents}.
   *
   * @param key {@link String}
   * @param lastModified {@link String}
   * @param etag {@link String}
   * @param size {@link String}
   * @param storageClass {@link String}
   * @param owner {@link Owner}
   */
  public BucketContents(final String key,
      final String lastModified,
      final String etag,
      final String size,
      final String storageClass,
      final Owner owner) {
    this.key = key;
    this.lastModified = lastModified;
    this.etag = etag;
    this.size = size;
    this.storageClass = storageClass;
    this.owner = owner;
  }

  public String getKey() {
    return key;
  }

  public String getLastModified() {
    return lastModified;
  }

  public String getEtag() {
    return etag;
  }

  public String getSize() {
    return size;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public Owner getOwner() {
    return owner;
  }
}
