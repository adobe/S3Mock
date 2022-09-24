/*
 *  Copyright 2017-2022 Adobe.
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
package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

internal class ErrorResponsesV2IT : S3TestBase() {
  /**
   * Verifies that `NO_SUCH_KEY` is returned in Error Response if `getObject`
   * on a non-existing Object.
   */
  @Test
  fun nonExistingObject(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val req = GetObjectRequest.builder().bucket(bucketName).key("NoSuchKey.json").build()
    assertThatThrownBy { s3ClientV2.getObject(req) }.isInstanceOf(
      NoSuchKeyException::class.java
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  companion object {
    private const val NO_SUCH_KEY = "The specified key does not exist."
  }
}
