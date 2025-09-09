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

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Bucket.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record Bucket(
    @JsonProperty("BucketRegion") String bucketRegion,
    @JsonProperty("CreationDate") String creationDate,
    @JsonProperty("Name") String name,
    @JsonIgnore Path path
) {

  public static Bucket from(BucketMetadata bucketMetadata) {
    if (bucketMetadata == null) {
      return null;
    }
    return new Bucket(bucketMetadata.bucketRegion,
        bucketMetadata.creationDate,
        bucketMetadata.name,
        bucketMetadata.path
    );
  }
}
