/*
 *  Copyright 2017-2024 Adobe.
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
import software.amazon.awssdk.core.checksums.Algorithm;

public enum ChecksumAlgorithm {
  CRC32("CRC32"),
  CRC32C("CRC32C"),
  SHA1("SHA1"),
  SHA256("SHA256");

  private final String value;

  @JsonCreator
  ChecksumAlgorithm(String value) {
    this.value = value;
  }

  public static ChecksumAlgorithm fromString(String value) {
    return switch (value) {
      case "sha256", "SHA256" -> SHA256;
      case "sha1", "SHA1" -> SHA1;
      case "crc32", "CRC32" -> CRC32;
      case "crc32c", "CRC32C" -> CRC32C;
      default -> null;
    };
  }

  public static ChecksumAlgorithm fromHeader(String value) {
    return switch (value) {
      case "x-amz-checksum-sha256" -> SHA256;
      case "x-amz-checksum-sha1" -> SHA1;
      case "x-amz-checksum-crc32" -> CRC32;
      case "x-amz-checksum-crc32c" -> CRC32C;
      default -> null;
    };
  }

  public Algorithm toAlgorithm() {
    return switch (this) {
      case CRC32 -> Algorithm.CRC32;
      case CRC32C -> Algorithm.CRC32C;
      case SHA1 -> Algorithm.SHA1;
      case SHA256 -> Algorithm.SHA256;
    };
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(this.value);
  }
}
