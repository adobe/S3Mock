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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

@JsonRootName("GetObjectAttributesOutput")
public record GetObjectAttributesOutput(
    @JsonProperty("Checksum")
    Checksum checksum,
    @JsonProperty("ETag")
    String etag,
    @JsonProperty("ObjectParts")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<GetObjectAttributesParts> objectParts,
    @JsonProperty("ObjectSize")
    Long objectSize,
    @JsonProperty("StorageClass")
    StorageClass storageClass,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns")
    String xmlns
) {

  public GetObjectAttributesOutput {
    etag = normalizeEtag(etag);
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public GetObjectAttributesOutput(Checksum checksum, String etag,
                                   List<GetObjectAttributesParts> objectParts, Long objectSize,
                                   StorageClass storageClass) {
    this(checksum, etag, objectParts, objectSize, storageClass, null);
  }

  GetObjectAttributesOutput from(S3ObjectMetadata metadata) {
    return new GetObjectAttributesOutput(null,
        metadata.etag(),
        null,
        Long.valueOf(metadata.size()),
        metadata.storageClass(),
        null);
  }
}
