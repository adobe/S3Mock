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
package com.adobe.testing.s3mock.util

import org.junit.jupiter.api.TestInfo
import java.io.File
import java.util.Objects

object TestUtil {
  fun getTestFile(testInfo: TestInfo, name: String?): File {
    val testClass = testInfo.testClass.get()
    val packageName = testClass.getPackage().name
    val className = testClass.simpleName
    val methodName = testInfo.testMethod.get().name
    val fileName = "${packageName.replace(".", "/")}/${className}_${methodName}_$name"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getPayloadFile(testInfo: TestInfo, name: String?): File {
    val testClass = testInfo.testClass.get()
    val packageName = testClass.getPackage().name
    val className = testClass.simpleName
    val fileName = "${packageName.replace(".", "/")}/${className}_payload_$name"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getFileFromPackage(testInfo: TestInfo, name: String?): File {
    val testClass = testInfo.testClass.get()
    val packageName = testClass.getPackage().name
    val fileName = "${packageName.replace(".", "/")}/$name"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getSampleFile(testInfo: TestInfo): File {
    return getFileFromClasspath(testInfo, "sampleFile.txt")
  }

  fun getFileFromClasspath(testInfo: TestInfo, fileName: String?): File {
    val testClass = testInfo.testClass.get()
    val classLoader = testClass.classLoader
    return File(Objects.requireNonNull(classLoader.getResource(fileName)).file)
  }
}
