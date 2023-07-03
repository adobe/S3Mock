/*
 *  Copyright 2017-2023 Adobe.
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

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.replace;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.TestInfo;
import org.xmlunit.assertj3.XmlAssert;

/**
 * Utility class to test serialization and deserialization.
 *
 * <p>
 * Tests have to follow the pattern:
 * Supply a file with the expected serialized data that matches the pattern
 * "package/ClassName_methodName.xml".
 * Call these methods with their respective {@link TestInfo} so the file can be found.
 * </p>
 *
 * <p>
 * Example:
 * {@link DeleteResultTest#testSerialization} provides the file
 * "src/test/resources/com/adobe/testing/s3mock/dto/DeleteResultTest_testSerialization.xml"
 * </p>
 */
class DtoTestUtil {

  private static final ObjectMapper MAPPER = XmlMapper.builder()
      .findAndAddModules()
      .build();

  /**
   * Finds and reads the test file, serializes the iut and asserts the contents are the same.
   */
  static void serializeAndAssert(Object iut, TestInfo testInfo) throws IOException {
    var out = MAPPER.writeValueAsString(iut);
    assertThat(out).isNotNull();
    var expected = getExpected(testInfo);
    XmlAssert.assertThat(out)
        .and(expected)
        .ignoreChildNodesOrder()
        .ignoreWhitespace()
        .ignoreComments()
        .areIdentical();
  }

  /**
   * Finds and reads the test file and returns its contents deserialized as T.
   */
  static <T> T deserialize(Class<T> clazz, TestInfo testInfo) throws IOException {
    var toDeserialize = getFile(testInfo);
    assertThat(toDeserialize).exists();
    return MAPPER.readValue(toDeserialize, clazz);
  }

  /**
   * Reads the test file and returns its contents.
   */
  static String getExpected(TestInfo testInfo) throws IOException {
    var file = getFile(testInfo);
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }

  static File getFile(TestInfo testInfo) {
    var testClass = testInfo.getTestClass().get();
    var packageName = testClass.getPackage().getName();
    var className = testClass.getSimpleName();
    var methodName = testInfo.getTestMethod().get().getName();
    var fileName = format("%s/%s_%s.xml", replace(packageName, ".", "/"), className, methodName);

    var classLoader = testClass.getClassLoader();
    return new File(Objects.requireNonNull(classLoader.getResource(fileName)).getFile());
  }
}
