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
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteMarkerEntry.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record DeleteMarkerEntry(
    @JsonProperty("IsLatest") Boolean isLatest,
    @JsonProperty("Key") String key,
    @JsonProperty("LastModified") String lastModified,
    @JsonProperty("Owner") Owner owner,
    @JsonProperty("VersionId") String versionId
) {

  public static DeleteMarkerEntry from(S3ObjectMetadata s3ObjectMetadata, boolean isLatest) {
    return new DeleteMarkerEntry(isLatest,
        s3ObjectMetadata.key(),
        s3ObjectMetadata.modificationDate(),
        s3ObjectMetadata.owner(),
        s3ObjectMetadata.versionId());
  }
}
