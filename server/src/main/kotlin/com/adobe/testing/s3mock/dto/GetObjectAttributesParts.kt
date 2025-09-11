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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributesParts.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record GetObjectAttributesParts(
    @JsonProperty("IsTruncated") boolean isTruncated,
    @JsonProperty("MaxParts") int maxParts,
    @JsonProperty("NextPartNumberMarker") int nextPartNumberMarker,
    @JsonProperty("PartNumberMarker") int partNumberMarker,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Parts") List<ObjectPart> parts,
    @JsonProperty("TotalPartsCount") int totalPartsCount
) {

}
