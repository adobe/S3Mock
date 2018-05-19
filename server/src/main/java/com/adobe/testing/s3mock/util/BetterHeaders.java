/*
 *  Copyright 2017-2018 Adobe.
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

package com.adobe.testing.s3mock.util;

/**
 * Holds Header used in HTTP requests from AWS S3 Client.
 */
public final class BetterHeaders {

  private static final String NOT = "!";

  public static final String SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID =
      "x-amz-server-side-encryption-aws-kms-key-id";

  public static final String SERVER_SIDE_ENCRYPTION = "x-amz-server-side-encryption";
  public static final String NOT_SERVER_SIDE_ENCRYPTION = NOT + SERVER_SIDE_ENCRYPTION;

  public static final String RANGE = "Range";

  public static final String COPY_SOURCE = "x-amz-copy-source";
  public static final String NOT_COPY_SOURCE = NOT + COPY_SOURCE;

  public static final String COPY_SOURCE_RANGE = "x-amz-copy-source-range";
  public static final String NOT_COPY_SOURCE_RANGE = NOT + COPY_SOURCE_RANGE;

  private BetterHeaders() {
    // empty private constructor
  }
}
