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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jspecify.annotations.Nullable;

/**
 * Last validation: 2025-04.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_OwnershipControlsRule.html">API Reference</a>.
 */
public enum ObjectOwnership {

  BUCKET_OWNER_PREFERRED("BucketOwnerPreferred"),
  OBJECT_WRITER("ObjectWriter"),
  BUCKET_OWNER_ENFORCED("BucketOwnerEnforced");

  private final String value;

  ObjectOwnership(String value) {
    this.value = value;
  }

  @JsonCreator
  @Nullable
  public static ObjectOwnership fromValue(String value) {
    return switch (value) {
      case "BucketOwnerPreferred" -> BUCKET_OWNER_PREFERRED;
      case "ObjectWriter" -> OBJECT_WRITER;
      case "BucketOwnerEnforced" -> BUCKET_OWNER_ENFORCED;
      default -> null;
    };
  }

  @Override
  @JsonValue
  public String toString() {
    return this.value;
  }
}
