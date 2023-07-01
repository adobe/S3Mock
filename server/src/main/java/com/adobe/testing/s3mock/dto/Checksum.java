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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html">API Reference</a>.
 */
@JsonRootName("Checksum")
public class Checksum {

  @JsonProperty("ChecksumCRC32")
  private String checksumCRC32;

  @JsonProperty("ChecksumCRC32C")
  private String checksumCRC32C;

  @JsonProperty("ChecksumSHA1")
  private String checksumSHA1;

  @JsonProperty("ChecksumSHA256")
  private String checksumSHA256;

  public Checksum(String checksumCRC32, String checksumCRC32C, String checksumSHA1,
      String checksumSHA256) {
    this.checksumCRC32 = checksumCRC32;
    this.checksumCRC32C = checksumCRC32C;
    this.checksumSHA1 = checksumSHA1;
    this.checksumSHA256 = checksumSHA256;
  }
}
