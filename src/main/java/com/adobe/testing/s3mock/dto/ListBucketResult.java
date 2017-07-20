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

import com.adobe.testing.s3mock.domain.BucketContents;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;

/**
 * Represents a result of listing objects that reside in a Bucket.
 */
@XStreamAlias("ListBucketResult")
@XStreamInclude({BucketContents.class})
public class ListBucketResult implements Serializable {
  @XStreamAlias("Name")
  private String name;

  @XStreamAlias("Prefix")
  private String prefix;

  @XStreamAlias("Marker")
  private String marker;

  @XStreamAlias("MaxKeys")
  private int maxKeys;

  @XStreamAlias("IsTruncated")
  private boolean isTruncated;

  @XStreamImplicit(itemFieldName = "Contents")
  private List<BucketContents> contents;

  @XStreamAlias("CommonPrefixes")
  private String commonPrefixes;

  /**
   * Constructs a new {@link ListBucketResult}.
   *
   */
  public ListBucketResult() {
    // empty
  }

  /**
   * Constructs a new {@link ListBucketResult}.
   *
   * @param name {@link String}
   * @param prefix {@link String}
   * @param marker {@link String}
   * @param maxKeys {@link String}
   * @param isTruncated {@link Boolean}
   * @param contents {@link List}
   * @param commonPrefixes {@link String}
   */
  public ListBucketResult(final String name,
      final String prefix,
      final String marker,
      final String maxKeys,
      final boolean isTruncated,
      final List<BucketContents> contents,
      final String commonPrefixes) {
    super();
    this.name = name;
    this.prefix = prefix;
    this.marker = marker;
    this.maxKeys = Integer.valueOf(maxKeys);
    this.isTruncated = isTruncated;
    this.contents = new ArrayList<>();
    this.contents.addAll(contents);
    this.commonPrefixes = commonPrefixes;
  }

  /**
   * @return the name
   */
  @XmlElement(name = "Name")
  public String getName() {
    return name;
  }

  /**
   * @return the marker
   */
  @XmlElement(name = "Marker")
  public String getMarker() {
    return marker;
  }

  /**
   * @return the maxKeys
   */
  @XmlElement(name = "MaxKeys")
  public String getMaxKeys() {
    return String.valueOf(maxKeys);
  }

  /**
   * @return the isTruncated
   */
  @XmlElement(name = "IsTruncated")
  public boolean isTruncated() {
    return isTruncated;
  }

  /**
   * @return the contents
   */
  public List<BucketContents> getContents() {
    return contents;
  }

  /**
   * set the contents
   *
   * @param contents {@link List}
   */
  public void setContents(final List<BucketContents> contents) {
    this.contents = contents;
  }

  /**
   * @return the commonPrefixes
   */
  @XmlElement(name = "CommonPrefixes")
  public String getCommonPrefixes() {
    return commonPrefixes;
  }
}
