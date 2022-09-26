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

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html">API Reference</a>.
 * See x-amz-object-attributes
 */
public enum ObjectAttributes {
  ETAG("ETag"),
  CHECKSUM("Checksum"),
  OBJECT_PARTS("ObjectParts"),
  STORAGE_CLASS("StorageClass"),
  OBJECT_SIZE("ObjectSize");

  private final String value;

  ObjectAttributes(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
