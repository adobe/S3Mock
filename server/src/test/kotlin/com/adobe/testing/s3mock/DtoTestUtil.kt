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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestInfo
import org.skyscreamer.jsonassert.JSONAssert
import org.xmlunit.assertj3.XmlAssert
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.dataformat.xml.XmlMapper
import tools.jackson.dataformat.xml.XmlReadFeature
import tools.jackson.dataformat.xml.XmlWriteFeature
import tools.jackson.module.kotlin.KotlinModule
import java.io.File
import java.io.IOException
import java.util.Objects

/**
 * Utility class to test serialization and deserialization.
 *
 * Tests have to follow the pattern:
 * Supply a file with the expected serialized data that matches the pattern
 * "package/ClassName_methodName.xml".
 * Call these methods with their respective [org.junit.jupiter.api.TestInfo] so the file can be found.
 *
 * Example:
 * [com.adobe.testing.s3mock.dto.DeleteResultTest.testSerialization] provides the file
 * "src/test/resources/com/adobe/testing/s3mock/dto/DeleteResultTest_testSerialization.xml"
 *
 */
object DtoTestUtil {
  private val XML_MAPPER: XmlMapper = XmlMapper.builder()
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

  private val JSON_MAPPER: ObjectMapper = JsonMapper.builder()
    // Ensure Kotlin/JavaTime/etc. modules are discovered similarly to Boot
    .addModule(KotlinModule.Builder().build())
    .findAndAddModules()
    // Align with Boot defaults
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()

  /**
   * Finds and reads the XML test file, serializes the iut and asserts the contents are the same.
   */
  @JvmStatic
  @Throws(IOException::class)
  fun serializeAndAssertXML(iut: Any?, testInfo: TestInfo) {
    val out = XML_MAPPER.writeValueAsString(iut)
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
   * Finds and reads the test file, serializes the iut and asserts the contents are the same.
   */
  @JvmStatic
  @Throws(IOException::class)
  fun serializeAndAssertJSON(iut: Any?, testInfo: TestInfo) {
    val out = JSON_MAPPER.writeValueAsString(iut)
    assertThat(out).isNotNull()
    val expected = getExpected(testInfo, "json")
    JSONAssert.assertEquals(expected, out, true)
  }

  /**
   * Finds and reads the test file and returns its contents deserialized as T.
   */
  @Throws(IOException::class)
  fun <T> deserializeXML(clazz: Class<T>, testInfo: TestInfo): T {
    val toDeserialize = getFile(testInfo, "xml")
    assertThat(toDeserialize).exists()
    return XML_MAPPER.readValue(toDeserialize, clazz)
  }

  /**
   * Finds and reads the test file and returns its contents deserialized as T.
   */
  @Throws(IOException::class)
  fun <T> deserializeJSON(clazz: Class<T>, testInfo: TestInfo): T {
    val toDeserialize = getFile(testInfo, "json")
    assertThat(toDeserialize).exists()
    return JSON_MAPPER.readValue(toDeserialize, clazz)
  }

  /**
   * Reads the test file and returns its contents.
   */
  @Throws(IOException::class)
  fun getExpected(testInfo: TestInfo, extension: String = "xml"): String {
    val file = getFile(testInfo, extension)
    return file.readText()
  }

  private fun getFile(testInfo: TestInfo, extension: String = "xml"): File {
    val testClass = testInfo.testClass.get()
    val packageName = testClass.getPackage().name
    val className = testClass.simpleName
    val methodName = testInfo.testMethod.get().name
    val fileName = "${packageName.replace(".", "/")}/${className}_${methodName}.$extension"
    val classLoader = testClass.classLoader
    return File(Objects.requireNonNull(classLoader.getResource(fileName)).file)
  }
}
