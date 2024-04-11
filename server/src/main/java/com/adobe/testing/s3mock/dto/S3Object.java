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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class representing an Object on S3.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html">API Reference</a>
 */
public record S3Object(
    @JsonProperty("Key")
    String key,
    @JsonProperty("LastModified")
    String lastModified,
    @JsonProperty("ETag")
    String etag,
    @JsonProperty("Size")
    String size,
    @JsonProperty("StorageClass")
    StorageClass storageClass,
    @JsonProperty("Owner")
    Owner owner,
    @JsonProperty("ChecksumAlgorithm")
    ChecksumAlgorithm checksumAlgorithm
) {

  public S3Object {
    etag = normalizeEtag(etag);
  }

  public static S3Object from(S3ObjectMetadata s3ObjectMetadata) {
    return new S3Object(s3ObjectMetadata.key(),
        s3ObjectMetadata.modificationDate(),
        s3ObjectMetadata.etag(),
        s3ObjectMetadata.size(),
        s3ObjectMetadata.storageClass(),
        s3ObjectMetadata.owner(),
        s3ObjectMetadata.checksumAlgorithm());
  }
}
