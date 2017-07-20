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

import com.adobe.testing.s3mock.dto.Owner;
import com.amazonaws.services.s3.model.StorageClass;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import javax.xml.bind.annotation.XmlElement;

/**
 * Contents are the XMLElements of ListBucketResult see
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
 */
@XStreamAlias("Contents")
public class BucketContents {
  @XStreamAlias("Key")
  private String key;

  @XStreamAlias("LastModified")
  private String lastModified;

  @XStreamAlias("ETag")
  private String etag;

  @XStreamAlias("Size")
  private String size;

  @XStreamAlias("StorageClass")
  private String storageClass;

  @XStreamAlias("Owner")
  private Owner owner;

  /**
   *
   * Create XMLElement of ListBucketResult
   *
   * Constructs a new {@link BucketContents}.
   *
   */
  public BucketContents() {
    // empty here
  }

  /**
   *
   * Create XMLElement of ListBucketResult
   *
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
      final StorageClass storageClass,
      final Owner owner) {
    super();
    this.key = key;
    this.lastModified = lastModified;
    this.etag = etag;
    this.size = size;
    this.storageClass = storageClass.name();
    this.owner = owner;
  }

  /**
   * @return the key
   */
  @XmlElement(name = "Key")
  public String getKey() {
    return key;
  }

  /**
   * @return the lastModified
   */
  @XmlElement(name = "LastModified")
  public String getLastModified() {
    return lastModified;
  }

  /**
   * @return the etag
   */
  @XmlElement(name = "ETag")
  public String getEtag() {
    return etag;
  }

  /**
   * @return the size
   */
  @XmlElement(name = "Size")
  public String getSize() {
    return size;
  }

  /**
   * @return the storageClass
   */
  @XmlElement(name = "StorageClass")
  public String getStorageClass() {
    return storageClass;
  }

  /**
   * @return the owner
   */
  @XmlElement(name = "Owner")
  public Owner getOwner() {
    return owner;
  }
}
