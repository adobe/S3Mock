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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.Tagging
import org.springframework.core.convert.converter.Converter
import tools.jackson.core.JacksonException
import tools.jackson.dataformat.xml.XmlMapper

/**
 * Converts values of the [com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING] which is sent by the Amazon client.
 * Example: x-amz-tagging: tag1=value1&tag2=value2
 *
 * It also converts XML tags into a list of [Tag] objects.
 * Example: '<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></TagSet></Tagging>'
 *
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html)
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html)
 */
class TaggingHeaderConverter(private val xmlMapper: XmlMapper) : Converter<String, List<Tag>> {

  override fun convert(source: String): List<Tag>? {
    val value = source.trim()
    return when {
      value.startsWith(XML_START) && value.endsWith(XML_END) -> convertTagXml(value)
      else -> convertTagPairs(value)
    }
  }

  private fun convertTagXml(source: String): List<Tag>? =
    try {
      val tagging = xmlMapper.readValue(source, Tagging::class.java)
      tagging.tagSet.tags.takeIf { it.isNotEmpty() }
    } catch (e: JacksonException) {
      throw IllegalArgumentException("Failed to parse XML tags from header: $source", e)
    }

  companion object {
    private const val XML_START = "<"
    private const val XML_END = ">"

    private fun convertTagPairs(source: String): List<Tag>? {
      // header format: tag1=value1&tag2=value2
      val tags = source
        .split('&')
        .filter { it.isNotBlank() }
        .map(::Tag)

      return tags.takeIf { it.isNotEmpty() }
    }
  }
}
