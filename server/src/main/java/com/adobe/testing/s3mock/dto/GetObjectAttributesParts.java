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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributesParts.html">API Reference</a>.
 */
@JsonRootName("GetObjectAttributesParts")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class GetObjectAttributesParts {

  @JsonProperty("MaxParts")
  private int maxParts;

  @JsonProperty("IsTruncated")
  private boolean isTruncated;

  @JsonProperty("NextPartNumberMarker")
  private int nextPartNumberMarker;

  @JsonProperty("PartNumberMarker")
  private int partNumberMarker;

  @JsonProperty("TotalPartsCount")
  private int totalPartsCount;

  @JsonProperty("Parts")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<ObjectPart> parts;

  public GetObjectAttributesParts(int maxParts, boolean isTruncated, int nextPartNumberMarker,
      int partNumberMarker, int totalPartsCount, List<ObjectPart> parts) {
    this.maxParts = maxParts;
    this.isTruncated = isTruncated;
    this.nextPartNumberMarker = nextPartNumberMarker;
    this.partNumberMarker = partNumberMarker;
    this.totalPartsCount = totalPartsCount;
    this.parts = parts;
  }
}
