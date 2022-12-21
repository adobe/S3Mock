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

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;
import software.amazon.awssdk.regions.Region;

/**
 * Get Bucket location result.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html">API Reference</a>
 */
@JsonRootName("LocationConstraint")
public class LocationConstraint {
  @JsonSerialize(using = RegionSerializer.class)
  @JacksonXmlText
  private final Region region;

  public LocationConstraint(Region region) {
    this.region = region;
  }
}
