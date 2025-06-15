/*
 *  Copyright 2017-2025 Adobe.
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

import org.jspecify.annotations.Nullable;

/**
 * Last validation: 2025-04.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl">API Reference</a>
 */
public enum ObjectCannedACL {
  PRIVATE("private"),

  PUBLIC_READ("public-read"),

  PUBLIC_READ_WRITE("public-read-write"),

  AUTHENTICATED_READ("authenticated-read"),

  AWS_EXEC_READ("aws-exec-read"),

  BUCKET_OWNER_READ("bucket-owner-read"),

  BUCKET_OWNER_FULL_CONTROL("bucket-owner-full-control");

  private final String value;

  ObjectCannedACL(String value) {
    this.value = value;
  }

  @Nullable
  public static ObjectCannedACL fromValue(String value) {
    return switch (value) {
      case "private" -> PRIVATE;
      case "public-read" -> PUBLIC_READ;
      case "public-read-write" -> PUBLIC_READ_WRITE;
      case "authenticated-read" -> AUTHENTICATED_READ;
      case "aws-exec-read" -> AWS_EXEC_READ;
      case "bucket-owner-read" -> BUCKET_OWNER_READ;
      case "bucket-owner-full-control" -> BUCKET_OWNER_FULL_CONTROL;
      default -> null;
    };
  }

  @Override
  public String toString() {
    return this.value;
  }
}
