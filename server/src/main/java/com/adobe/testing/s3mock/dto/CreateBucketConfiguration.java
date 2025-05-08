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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucketConfiguration.html">API Reference</a>.
 */
@S3Verified(year = 2025)
@JsonRootName("CreateBucketConfiguration")
public record CreateBucketConfiguration(
    @JsonProperty("Bucket") BucketInfo bucket,
    @JsonProperty("Location") LocationInfo location,
    @JsonSerialize(using = LocationConstraintSerializer.class)
    @JsonDeserialize(using = LocationConstraintDeserializer.class)
    @JsonProperty("LocationConstraint") LocationConstraint locationConstraint,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {
  public CreateBucketConfiguration {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public CreateBucketConfiguration(BucketInfo bucket,
       LocationInfo location,
       LocationConstraint locationConstraint) {
    this(bucket, location, locationConstraint, null);
  }
}
