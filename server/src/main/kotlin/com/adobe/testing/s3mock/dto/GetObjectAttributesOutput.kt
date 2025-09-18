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
package com.adobe.testing.s3mock.dto

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
 */
@S3Verified(year = 2025)
@JsonRootName("GetObjectAttributesOutput", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
data class GetObjectAttributesOutput(
  @param:JsonProperty("Checksum", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksum: Checksum?,
  @param:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val etag: String?,
  @param:JacksonXmlElementWrapper(useWrapping = false)
  @param:JsonProperty("ObjectParts", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val objectParts: List<GetObjectAttributesParts>?,
  @param:JsonProperty("ObjectSize", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val objectSize: Long?,
  @param:JsonProperty("StorageClass", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val storageClass: StorageClass?,
) {
  fun from(metadata: S3ObjectMetadata): GetObjectAttributesOutput {
    return GetObjectAttributesOutput(
      Checksum.from(metadata),
      normalizeEtag(metadata.etag),
      null,
      metadata.size.toLong(),
      metadata.storageClass,
    )
  }
}
