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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletedObject.html">API Reference</a>.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record DeletedS3Object(
    @JsonProperty("Key")
    String key,
    @JsonProperty("VersionId")
    String versionId,
    @JsonProperty("DeleteMarker")
    Boolean deleteMarker,
    @JsonProperty("DeleteMarkerVersionId")
    String deleteMarkerVersionId
) {

  public static DeletedS3Object from(S3ObjectIdentifier s3ObjectIdentifier) {
    return new DeletedS3Object(
        s3ObjectIdentifier.key(),
        s3ObjectIdentifier.versionId(),
        null,
        null
    );
  }
}
