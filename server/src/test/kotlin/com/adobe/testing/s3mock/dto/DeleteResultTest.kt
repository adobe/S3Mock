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

import com.adobe.testing.s3mock.DtoTestUtil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class DeleteResultTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val deleted = mutableListOf<DeletedS3Object>()
    val errors = mutableListOf<Error>()
    repeat(2) {
      val deletedObject = S3ObjectIdentifier(
          "key$it",
          "etag$it",
          "lastModifiedTime$it",
          "size$it",
          "versionId$it"
      )
      deleted.add(DeletedS3Object.from(deletedObject))
    }
    errors.add(Error("errorCode", "key3", "errorMessage", "versionId3"))
    val iut = DeleteResult(deleted, errors)
    DtoTestUtil.serializeAndAssertXML(iut, testInfo)
  }
}
