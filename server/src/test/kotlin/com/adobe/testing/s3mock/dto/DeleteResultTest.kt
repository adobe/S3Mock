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
package com.adobe.testing.s3mock.dto

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException

internal class DeleteResultTest {
  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val iut = DeleteResult(ArrayList(), ArrayList())
    assertThat(iut).isNotNull()
    val count = 2
    for (i in 0 until count) {
      val deletedObject = S3ObjectIdentifier(
          "key$i",
          "etag$i",
          "lastModifiedTime$i",
          "size$i",
          "versionId$i"
      )
      iut.addDeletedObject(DeletedS3Object.from(deletedObject))
    }
    iut.addError(Error("errorCode", "key3", "errorMessage", "versionId3"))
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
