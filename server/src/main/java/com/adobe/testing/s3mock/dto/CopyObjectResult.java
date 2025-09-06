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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObjectResult.html">API Reference</a>.
 */
@S3Verified(year = 2025)
@JsonRootName("CopyObjectResult")
public record CopyObjectResult(
    @JsonProperty("ETag") String etag,
    @JsonProperty("LastModified") String lastModified,
    // workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {
  public CopyObjectResult {
    etag = normalizeEtag(etag);
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public CopyObjectResult(String lastModified, String etag) {
    this(etag, lastModified, null);
  }

  public CopyObjectResult(S3ObjectMetadata metadata) {
    this(metadata.modificationDate(), metadata.etag());
  }
}
