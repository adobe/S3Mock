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
package com.adobe.testing.s3mock

import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator

internal abstract class BaseControllerTest {
  companion object {
    val MAPPER: XmlMapper = XmlMapper.builder()
      .findAndAddModules()
      .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
      .enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
      .enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
      .build()

    init {
      MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      MAPPER.factory.xmlOutputFactory
        .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)
    }
  }
}
