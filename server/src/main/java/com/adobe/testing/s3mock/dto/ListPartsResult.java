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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * List-Parts result with some hard-coded values as this is sufficient for now.
 */
@JsonRootName("ListPartsResult")
public class ListPartsResult {

  @JsonProperty("Bucket")
  private final String bucket;

  @JsonProperty("Key")
  private final String key;

  @JsonProperty("UploadId")
  private final String uploadId;

  @JsonProperty("PartNumberMarker")
  private final String partnumber = "0";

  @JsonProperty("NextPartNumberMarker")
  private final String nextpartnumber = "1";

  @JsonProperty("IsTruncated")
  private final boolean truncated = false;

  @JsonProperty("StorageClass")
  private final String storageClass = "STANDARD";

  @JsonProperty("Parts")
  private final List<Part> parts = new ArrayList<>();

  /**
   * Constructs a new {@link ListPartsResult}.
   *
   * @param bucketName of the bucket.
   * @param fileName of the file.
   * @param uploadId of the multipart upload.
   * @param parts bla
   */
  public ListPartsResult(final String bucketName,
                         final String fileName,
                         final String uploadId,
                         final Part... parts) {
    bucket = bucketName;
    key = fileName;
    this.uploadId = uploadId;
    this.parts.addAll(Arrays.asList(parts));
  }
}
