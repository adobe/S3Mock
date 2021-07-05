/*
 *  Copyright 2017-2021 Adobe.
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

import static org.apache.commons.lang3.StringUtils.replace;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.TestInfo;
import org.xmlunit.assertj3.XmlAssert;

class DtoTestUtil {

  private static final ObjectMapper MAPPER = new XmlMapper();

  static void serializeAndAssert(Object iut, TestInfo testInfo) throws IOException {
    String out = MAPPER.writeValueAsString(iut);
    assertThat(out).isNotNull();
    String expected = getExpected(testInfo);
    XmlAssert.assertThat(out).and(expected)
        .ignoreChildNodesOrder()
        .ignoreWhitespace()
        .ignoreComments()
        .areIdentical();
  }

  /**
   * Reads a test file like "ListBucketResultV2Test_testSerialization.xml" and returns its contents
   */
  static String getExpected(TestInfo testInfo) throws IOException {
    Class<?> testClass = testInfo.getTestClass().get();
    String packageName = testClass.getPackage().getName();
    String className = testClass.getSimpleName();
    String methodName = testInfo.getTestMethod().get().getName();
    String fileName =
        String.format("%s/%s_%s.xml", replace(packageName, ".", "/"), className, methodName);

    ClassLoader classLoader = testClass.getClassLoader();
    File file = new File(classLoader.getResource(fileName).getFile());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }
}
