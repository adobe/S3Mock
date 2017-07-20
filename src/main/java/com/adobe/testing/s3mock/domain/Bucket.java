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
import java.nio.file.Path;

/**
 * DTO representing a bucket.
 */
@XStreamAlias("Bucket")
public class Bucket {
  @XStreamAlias("Name")
  private String name;

  @XStreamAlias("CreationDate")
  private String creationDate;

  private Path path;

  /**
   * Constructs a new {@link Bucket}.
   *
   * @param bucketPath path of bucket
   * @param name of bucket
   * @param creationDate date of creation.
   */
  public Bucket(final Path bucketPath, final String name, final String creationDate) {
    super();
    this.name = name;
    this.creationDate = creationDate;
    this.path = bucketPath;
  }

  /**
   * @return the creationDate
   */
  public String getCreationDate() {
    return creationDate;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param creationDate the creationDate to set
   */
  public void setCreationDate(final String creationDate) {
    this.creationDate = creationDate;
  }

  /**
   * @param name the name to set
   */
  public void setName(final String name) {
    this.name = name;
  }

  /**
   * @return the bucketPath
   */
  public Path getPath() {
    return path;
  }

  /**
   * @param bucketPath the bucketPath to set
   */
  public void setPath(final Path bucketPath) {
    this.path = bucketPath;
  }
}
