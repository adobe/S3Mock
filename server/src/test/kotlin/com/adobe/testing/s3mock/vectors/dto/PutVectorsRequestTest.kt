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

internal class PutVectorsRequestTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(PutVectorsRequest::class.java, testInfo)

    assertThat(iut.vectorBucketName).isEqualTo("my-vector-bucket")
    assertThat(iut.indexName).isEqualTo("my-index")
    assertThat(iut.vectors).hasSize(2)

    val v1 = iut.vectors!![0]
    assertThat(v1.key).isEqualTo("doc-1")
    assertThat(v1.data?.float32).containsExactly(1.0, 0.0, 0.0)
    assertThat(v1.metadata?.get("genre")).isEqualTo("drama")
    assertThat(v1.metadata?.get("year")).isEqualTo(2024)

    val v2 = iut.vectors!![1]
    assertThat(v2.key).isEqualTo("doc-2")
    assertThat(v2.data?.float32).containsExactly(0.0, 1.0, 0.0)
    assertThat(v2.metadata).isNull()
  }
}
