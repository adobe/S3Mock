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
package com.adobe.testing.s3mock.dto

import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestInfo
import org.xmlunit.assertj3.XmlAssert
import java.io.File
import java.io.IOException
import java.util.Objects

/**
 * Utility class to test serialization and deserialization.
 *
 *
 *
 * Tests have to follow the pattern:
 * Supply a file with the expected serialized data that matches the pattern
 * "package/ClassName_methodName.xml".
 * Call these methods with their respective [TestInfo] so the file can be found.
 *
 *
 *
 *
 * Example:
 * [DeleteResultTest.testSerialization] provides the file
 * "src/test/resources/com/adobe/testing/s3mock/dto/DeleteResultTest_testSerialization.xml"
 *
 */
internal object DtoTestUtil {
  private val MAPPER: XmlMapper = XmlMapper.builder()
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

  /**
   * Finds and reads the test file, serializes the iut and asserts the contents are the same.
   */
  @JvmStatic
  @Throws(IOException::class)
  fun serializeAndAssert(iut: Any?, testInfo: TestInfo) {
    val out = MAPPER.writeValueAsString(iut)
    assertThat(out).isNotNull()
    val expected = getExpected(testInfo)
    XmlAssert.assertThat(out)
      .and(expected)
      .ignoreChildNodesOrder()
      .ignoreWhitespace()
      .ignoreComments()
      .areIdentical()
  }

  /**
   * Finds and reads the test file and returns its contents deserialized as T.
   */
  @Throws(IOException::class)
  fun <T> deserialize(clazz: Class<T>, testInfo: TestInfo): T {
    val toDeserialize = getFile(testInfo)
    assertThat(toDeserialize).exists()
    return MAPPER.readValue(toDeserialize, clazz)
  }

  /**
   * Reads the test file and returns its contents.
   */
  @Throws(IOException::class)
  fun getExpected(testInfo: TestInfo): String {
    val file = getFile(testInfo)
    return file.readText()
  }

  private fun getFile(testInfo: TestInfo): File {
    val testClass = testInfo.testClass.get()
    val packageName = testClass.getPackage().name
    val className = testClass.simpleName
    val methodName = testInfo.testMethod.get().name
    val fileName = "${packageName.replace(".", "/")}/${className}_${methodName}.xml"
    val classLoader = testClass.classLoader
    return File(Objects.requireNonNull(classLoader.getResource(fileName)).file)
  }
}
