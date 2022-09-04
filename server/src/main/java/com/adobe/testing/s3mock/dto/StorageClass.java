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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html">API Reference</a>.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html">API Reference</a>.
 */
public enum StorageClass {
  STANDARD("STANDARD"),
  REDUCED_REDUNDANCY("REDUCED_REDUNDANCY"),
  GLACIER("GLACIER"),
  STANDARD_IA("STANDARD_IA"),
  ONEZONE_IA("ONEZONE_IA"),
  INTELLIGENT_TIERING("INTELLIGENT_TIERING"),
  DEEP_ARCHIVE("DEEP_ARCHIVE"),
  OUTPOSTS("OUTPOSTS"),
  GLACIER_IR("GLACIER_IR");

  private final String value;

  @JsonCreator
  StorageClass(String value) {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString() {
    return value;
  }
}
