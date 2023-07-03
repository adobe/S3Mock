/*
 *  Copyright 2017-2023 Adobe.
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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributesParts.html">API Reference</a>.
 */
@JsonRootName("GetObjectAttributesParts")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record GetObjectAttributesParts(
    @JsonProperty("MaxParts")
    int maxParts,
    @JsonProperty("IsTruncated")
    boolean isTruncated,
    @JsonProperty("NextPartNumberMarker")
    int nextPartNumberMarker,
    @JsonProperty("PartNumberMarker")
    int partNumberMarker,
    @JsonProperty("TotalPartsCount")
    int totalPartsCount,
    @JsonProperty("Parts")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<ObjectPart> parts
) {

}
