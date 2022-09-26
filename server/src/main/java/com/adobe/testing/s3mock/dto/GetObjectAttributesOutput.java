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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

@JsonRootName("GetObjectAttributesOutput")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class GetObjectAttributesOutput {

  @JsonProperty("Checksum")
  private Checksum checksum;

  @JsonProperty("ETag")
  private String etag;

  @JsonProperty("ObjectParts")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<GetObjectAttributesParts> objectParts;

  @JsonProperty("ObjectSize")
  private Long objectSize;

  @JsonProperty("StorageClass")
  private StorageClass storageClass;

  public GetObjectAttributesOutput(Checksum checksum, String etag,
      List<GetObjectAttributesParts> objectParts, Long objectSize, StorageClass storageClass) {
    this.checksum = checksum;
    this.etag = normalizeEtag(etag);
    this.objectParts = objectParts;
    this.objectSize = objectSize;
    this.storageClass = storageClass;
  }

  GetObjectAttributesOutput from(S3ObjectMetadata metadata) {
    return new GetObjectAttributesOutput(null,
        metadata.getEtag(),
        null,
        Long.valueOf(metadata.getSize()),
        null);
  }

}
