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

package com.adobe.testing.s3mock.util;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.replace;

import java.io.File;
import org.junit.jupiter.api.TestInfo;

public class TestUtil {

  public static File getTestFile(TestInfo testInfo, String name) {
    Class<?> testClass = testInfo.getTestClass().get();
    String packageName = testClass.getPackage().getName();
    String className = testClass.getSimpleName();
    String methodName = testInfo.getTestMethod().get().getName();
    String fileName =
        format("%s/%s_%s_%s", replace(packageName, ".", "/"), className, methodName, name);
    return getFileFromClasspath(testInfo, fileName);
  }

  public static File getPayloadFile(TestInfo testInfo, String name) {
    Class<?> testClass = testInfo.getTestClass().get();
    String packageName = testClass.getPackage().getName();
    String className = testClass.getSimpleName();
    String fileName =
        format("%s/%s_%s_%s", replace(packageName, ".", "/"), className, "payload", name);
    return getFileFromClasspath(testInfo, fileName);
  }

  public static File getSampleFile(TestInfo testInfo) {
    return getFileFromClasspath(testInfo, "sampleFile.txt");
  }

  public static File getFileFromClasspath(TestInfo testInfo, String fileName) {
    Class<?> testClass = testInfo.getTestClass().get();
    ClassLoader classLoader = testClass.getClassLoader();
    return new File(requireNonNull(classLoader.getResource(fileName)).getFile());
  }
}
