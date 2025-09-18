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
import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import tools.jackson.dataformat.xml.XmlMapper
import tools.jackson.dataformat.xml.XmlReadFeature
import tools.jackson.dataformat.xml.XmlWriteFeature

internal class TaggingHeaderConverterTest {

  private val iut = TaggingHeaderConverter(MAPPER)

  @Test
  fun `converts xml to tags`() {
    val XML = """<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></TagSet></Tagging>""".trimMargin()
    val actual = iut.convert(XML)
    assertThat(actual).isNotEmpty()
      .hasSize(2)
      .containsOnly(
        Tag("k1=v1"),
        Tag("k2=v2"),
      )
  }

  @Test
  fun `returns null for empty tags`() {
    val actual = iut.convert("")
    assertThat(actual).isNull()
  }

  @Test
  fun `converts single tag`() {
    val singleTag = tag(1)
    val actual = iut.convert(singleTag)
    assertThat(actual).isNotEmpty().hasSize(1)
    assertThat(requireNotNull(actual)).containsExactly(Tag(singleTag))
  }

  @Test
  fun `converts multiple tags`() {
    val tags = (0 until 5).map { tag(it) }
    val actual = iut.convert(tags.joinToString("&"))
    assertThat(actual)
      .isNotEmpty()
      .hasSize(5)
      .containsOnly(
        Tag(tag(0)),
        Tag(tag(1)),
        Tag(tag(2)),
        Tag(tag(3)),
        Tag(tag(4))
      )
  }

  private companion object {
    val MAPPER: XmlMapper = XmlMapper.builder()
      .findAndAddModules()
      .enable(XmlWriteFeature.WRITE_XML_DECLARATION)
      .enable(XmlWriteFeature.AUTO_DETECT_XSI_TYPE)
      .enable(XmlReadFeature.AUTO_DETECT_XSI_TYPE)
      .changeDefaultPropertyInclusion { it.withValueInclusion(JsonInclude.Include.NON_EMPTY) }
      .build().apply {
        tokenStreamFactory()
          .xmlOutputFactory
          .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)
      }
    fun tag(i: Int): String = "tag$i=value$i"
  }
}
