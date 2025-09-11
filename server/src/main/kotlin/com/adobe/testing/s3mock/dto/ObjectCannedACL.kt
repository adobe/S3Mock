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
package com.adobe.testing.s3mock.dto

/**
 * Last validation: 2025-04.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
 */
enum class ObjectCannedACL(private val value: String) {
  PRIVATE("private"),

  PUBLIC_READ("public-read"),

  PUBLIC_READ_WRITE("public-read-write"),

  AUTHENTICATED_READ("authenticated-read"),

  AWS_EXEC_READ("aws-exec-read"),

  BUCKET_OWNER_READ("bucket-owner-read"),

  BUCKET_OWNER_FULL_CONTROL("bucket-owner-full-control");

  override fun toString(): String {
    return this.value
  }

  companion object {
    fun fromValue(value: String): ObjectCannedACL? {
      return when (value) {
        "private" -> PRIVATE
        "public-read" -> PUBLIC_READ
        "public-read-write" -> PUBLIC_READ_WRITE
        "authenticated-read" -> AUTHENTICATED_READ
        "aws-exec-read" -> AWS_EXEC_READ
        "bucket-owner-read" -> BUCKET_OWNER_READ
        "bucket-owner-full-control" -> BUCKET_OWNER_FULL_CONTROL
        else -> null
      }
    }
  }
}
