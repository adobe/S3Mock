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

import static java.util.Objects.requireNonNull;

/**
 * Key request value object.
 * Removes the trailing slash extracted from paths by Spring.
 * Used in conjunction with the PathVariable extracted by using "{*key}" in the path pattern.
 * See {@link org.springframework.web.util.pattern.PathPattern}
 * Example path pattern: "/{bucketName:.+}/{*key}"
 * Example incoming path: "/my-bucket/prefix/before/my/key"
 * By declaring "{*key}", Spring extracts the absolute path "/prefix/before/my/key", but in S3, all
 * keys within a bucket are relative to the bucket, in this example "prefix/before/my/key".
 */
public class ObjectKey {

  private final String key;

  public ObjectKey(String key) {
    requireNonNull(key);
    if (key.startsWith("/")) {
      this.key = key.substring(1);
    } else {
      this.key = key;
    }
  }

  public String getKey() {
    return key;
  }
}
