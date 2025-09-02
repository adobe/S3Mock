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
package com.adobe.testing.s3mock.util

import org.junit.jupiter.api.TestInfo
import java.io.File

object TestUtil {
  fun getTestFile(testInfo: TestInfo, name: String?): File {
    val (packagePath, className, methodName) = testMeta(testInfo)
    val safeName = name.orEmpty()
    val fileName = "$packagePath/${className}_${methodName}_$safeName"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getPayloadFile(testInfo: TestInfo, name: String?): File {
    val (packagePath, className) = testMeta(testInfo)
    val safeName = name.orEmpty()
    val fileName = "$packagePath/${className}_payload_$safeName"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getFileFromPackage(testInfo: TestInfo, name: String?): File {
    val (packagePath) = testMeta(testInfo)
    val fileName = "$packagePath/${name.orEmpty()}"
    return getFileFromClasspath(testInfo, fileName)
  }

  fun getSampleFile(testInfo: TestInfo): File =
    getFileFromClasspath(testInfo, "sampleFile.txt")

  fun getFileFromClasspath(testInfo: TestInfo, fileName: String?): File {
    val testClass = requireNotNull(testInfo.testClass.orElse(null)) { "Test class is not present in TestInfo" }
    val resource = requireNotNull(testClass.classLoader.getResource(fileName)) { "Resource not found on classpath: $fileName" }
    return File(resource.toURI())
  }

  private fun testMeta(testInfo: TestInfo): Triple<String, String, String?> {
    val testClass = requireNotNull(testInfo.testClass.orElse(null)) { "Test class is not present in TestInfo" }
    val packagePath = testClass.packageName.replace('.', '/')
    val className = testClass.simpleName
    val methodName = testInfo.testMethod.orElse(null)?.name
    return Triple(packagePath, className, methodName)
  }
}
