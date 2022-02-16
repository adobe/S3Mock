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

package com.adobe.testing.s3mock.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("com.adobe.testing.s3mock.domain")
class DomainProperties {

  /**
   * True if files should be retained when S3Mock exits gracefully.
   * False to let S3Mock delete all files when S3Mock exits gracefully.
   */
  private boolean retainFilesOnExit;

  /**
   * The root directory to use. If omitted a default temp-dir will be used.
   */
  private String root;

  private Set<String> validKmsKeys = new HashSet<>();

  /**
   * A comma separated list of buckets that are to be created at startup.
   */
  private List<String> initialBuckets = new ArrayList<>();

  public List<String> getInitialBuckets() {
    return initialBuckets;
  }

  public void setInitialBuckets(List<String> initialBuckets) {
    this.initialBuckets = initialBuckets;
  }

  public boolean isRetainFilesOnExit() {
    return retainFilesOnExit;
  }

  public void setRetainFilesOnExit(boolean retainFilesOnExit) {
    this.retainFilesOnExit = retainFilesOnExit;
  }

  public String getRoot() {
    return root;
  }

  public void setRoot(String root) {
    this.root = root;
  }

  public Set<String> getValidKmsKeys() {
    return validKmsKeys;
  }

  public void setValidKmsKeys(Set<String> validKmsKeys) {
    this.validKmsKeys = validKmsKeys;
  }
}
