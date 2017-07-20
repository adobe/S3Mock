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

import static java.util.Objects.requireNonNull;

/**
 * Represents a S3 Object referenced by Bucket and Key.
 */
public final class ObjectRef {
  public static final String DELIMITER = "/";

  private final String bucket;
  private final String key;

  private ObjectRef(final String bucket, final String key) {
    this.bucket = bucket;
    this.key = key;
  }

  /**
   * Creates a {@link ObjectRef} expecting the given String represents the
   * source as {@code /{bucket}/{key}}.
   *
   * @param copySource The object references.
   *
   * @return the {@link ObjectRef}
   *
   * @throws IllegalArgumentException If {@code copySource} could not be parsed.
   * @throws NullPointerException If {@code copySource} is null.
   */
  public static ObjectRef from(final String copySource) {
    requireNonNull(copySource, "copySource == null");

    final String[] bucketAndKey = extractBucketAndKeyArray(copySource);

    return new ObjectRef(bucketAndKey[0], bucketAndKey[1]);

  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override public String toString() {
    final StringBuffer sb = new StringBuffer("ObjectRef{");
    sb.append("bucket='").append(bucket).append('\'');
    sb.append(", key='").append(key).append('\'');
    sb.append('}');
    return sb.toString();
  }

  private static String[] extractBucketAndKeyArray(final String copySource) {
    final String source = normalizeCopySource(copySource);
    final String[] bucketAndKey = source.split(DELIMITER);

    if (bucketAndKey.length != 2) {
      throw new IllegalArgumentException(
          "Expected a copySource as '/{bucket}/{key}' but got: " + copySource);
    }

    return bucketAndKey;
  }

  private static String normalizeCopySource(final String copySource) {
    if (copySource.startsWith("/")) {
      return copySource.substring(1);
    }
    return copySource;
  }
}
