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

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Class representing an Object on S3.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html">API Reference</a>
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class S3Object {

  @JsonProperty("Key")
  private String key;

  @JsonProperty("LastModified")
  private String lastModified;

  @JsonProperty("ETag")
  private String etag;

  @JsonProperty("Size")
  private String size;

  @JsonProperty("StorageClass")
  private StorageClass storageClass;

  @JsonProperty("Owner")
  private Owner owner;

  public S3Object() {
    // Jackson needs the default constructor for deserialization.
  }

  public S3Object(String key,
      final String lastModified,
      final String etag,
      final String size,
      final StorageClass storageClass,
      final Owner owner) {
    this.key = key;
    this.lastModified = lastModified;
    // make sure to store the etag correctly here, every usage depends on this...
    if (etag == null) {
      this.etag = etag;
    } else if (etag.startsWith("\"") && etag.endsWith("\"")) {
      this.etag = etag;
    } else {
      this.etag = String.format("\"%s\"", etag);
    }
    this.size = size;
    this.storageClass = storageClass;
    this.owner = owner;
  }

  public static S3Object from(S3ObjectMetadata s3ObjectMetadata) {
    return new S3Object(s3ObjectMetadata.getKey(),
        s3ObjectMetadata.getModificationDate(), s3ObjectMetadata.getEtag(),
        s3ObjectMetadata.getSize(), StorageClass.STANDARD, Owner.DEFAULT_OWNER);
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
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

  public StorageClass getStorageClass() {
    return storageClass;
  }

  public Owner getOwner() {
    return owner;
  }
}
