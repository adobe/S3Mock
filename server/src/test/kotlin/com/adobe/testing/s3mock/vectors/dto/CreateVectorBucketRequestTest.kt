/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.vectors.dto

import com.adobe.testing.s3mock.DtoTestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class CreateVectorBucketRequestTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(CreateVectorBucketRequest::class.java, testInfo)

    assertThat(iut.vectorBucketName).isEqualTo("my-vector-bucket")
    assertThat(iut.encryptionConfiguration?.sseType).isEqualTo("aws:kms")
    assertThat(iut.encryptionConfiguration?.kmsKeyArn)
      .isEqualTo("arn:aws:kms:us-east-1:123456789012:key/mrk-test")
    assertThat(iut.tags).containsEntry("env", "test").containsEntry("team", "data")
  }

  @Test
  fun testDeserializationMinimal(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(CreateVectorBucketRequest::class.java, testInfo)

    assertThat(iut.vectorBucketName).isEqualTo("my-vector-bucket")
    assertThat(iut.encryptionConfiguration).isNull()
    assertThat(iut.tags).isNull()
  }
}
