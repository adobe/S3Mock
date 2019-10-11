/*
 *  Copyright 2017-2019 Adobe.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import java.nio.file.Path;

/**
 * DTO representing a bucket.
 */
@JsonRootName("Bucket")
public class Bucket {

  @JsonProperty("Name")
  private String name;

  @JsonProperty("CreationDate")
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
    this.name = name;
    this.creationDate = creationDate;
    path = bucketPath;
  }

  /**
   * return the attribute creantionDate
   * @return this.creationDate
   */
  public String getCreationDate() {
    return creationDate;
  }

  /**
   * Method responsable to set the attribute setCreationDate using a string passed as parameter
   *
   * @param creationDate the string that represents the new creationDate
   */
  public void setCreationDate(final String creationDate) {
    this.creationDate = creationDate;
  }

  /**
   * Method that returns the name of the object.
   *
   * @return the attribute name.
   */
  public String getName() {
    return name;
  }

  /**
   * Method responsable for setting a new name for the object using a string that represents the new name.
   *
   * @param name string that represents the new name
   */
  public void setName(final String name) {
    this.name = name;
  }

  /**
   * Method that returns the path of the bucket on the sistem
   *
   * @return the attribute path.
   */
  public Path getPath() {
    return path;
  }

  /**
   * This method overwrite the path with a new bucketPath passed as parameter.
   *
   * @param bucketPath the bucketPath
   */
  public void setPath(final Path bucketPath) {
    path = bucketPath;
  }
}
