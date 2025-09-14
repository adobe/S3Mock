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

import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Date

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Part.html).
 */
class Part(
  @param:JsonProperty("PartNumber")
  val partNumber: Int,
  @JsonProperty("ETag") etag: String?,
  @param:JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  @param:JsonProperty("LastModified")
  val lastModified: Date,
  @param:JsonProperty("Size")
  val size: Long
) {
  constructor(partNumber: Int, etag: String?, size: Long) :
    this(partNumber, normalizeEtag(etag), Date(), size)

  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as Part

    if (partNumber != other.partNumber) return false
    if (size != other.size) return false
    if (lastModified != other.lastModified) return false
    if (etag != other.etag) return false

    return true
  }

  override fun hashCode(): Int {
    var result = partNumber
    result = 31 * result + size.hashCode()
    result = 31 * result + lastModified.hashCode()
    result = 31 * result + (etag?.hashCode() ?: 0)
    return result
  }


}
