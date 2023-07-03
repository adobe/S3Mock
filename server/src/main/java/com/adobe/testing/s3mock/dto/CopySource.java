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

import software.amazon.awssdk.utils.http.SdkHttpUtils;

/**
 * Represents a S3 Object referenced by Bucket and Key.
 */
public record CopySource(
    String bucket,
    String key
) {
  public static final String DELIMITER = "/";

  /**
   * Creates a {@link CopySource} expecting the given String represents the source as {@code
   * /{bucket}/{key}}.
   *
   * @param copySource The object references.
   *
   * @throws IllegalArgumentException If {@code copySource} could not be parsed.
   * @throws NullPointerException If {@code copySource} is null.
   */
  public CopySource(String copySource) {
    //inefficient duplicate parsing of incoming String, call to default constructor must be the
    //first statement...
    this(extractBucketAndKeyArray(SdkHttpUtils.urlDecode(copySource))[0],
        extractBucketAndKeyArray(SdkHttpUtils.urlDecode(copySource))[1]);
  }

  /**
   * we need to decode here because Spring does not do the decoding for RequestHeaders as it does
   * for path parameters.
   */
  private static String[] extractBucketAndKeyArray(final String copySource) {
    requireNonNull(copySource, "copySource == null");
    final String source = normalizeCopySource(copySource);
    final String[] bucketAndKey = source.split(DELIMITER, 2);

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
