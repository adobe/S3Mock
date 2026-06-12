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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.dto.PutInputVector
import com.adobe.testing.s3mock.dto.VectorData
import com.adobe.testing.s3mock.store.VectorBucketMetadata
import com.adobe.testing.s3mock.store.VectorIndexMetadata
import com.adobe.testing.s3mock.store.VectorStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.nio.file.Path
import java.util.UUID

internal class VectorServiceTest {
  private val vectorStore: VectorStore = mock()
  private val iut = VectorService(vectorStore)

  @Test
  fun `create vector bucket returns arn`() {
    whenever(vectorStore.doesVectorBucketExist("bucket")).thenReturn(false)
    whenever(vectorStore.createVectorBucket(any(), any(), any())).thenReturn(
      VectorBucketMetadata(
        vectorBucketName = "bucket",
        vectorBucketArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/bucket",
        creationTime = 1,
        path = Path.of("/tmp/bucket"),
      ),
    )

    val response = iut.createVectorBucket(CreateVectorBucketRequest(vectorBucketName = "bucket"))

    assertThat(response.vectorBucketArn).contains("bucket/bucket")
  }

  @Test
  fun `put vectors validates vector size`() {
    whenever(vectorStore.getIndex("bucket", "index")).thenReturn(
      VectorIndexMetadata(
        id = UUID.randomUUID(),
        indexName = "index",
        indexArn = "arn:aws:s3vectors:us-east-1:123456789012:bucket/bucket/index/index",
        vectorBucketName = "bucket",
        creationTime = 1,
        dataType = "float32",
        dimension = 3,
        distanceMetric = "euclidean",
        path = Path.of("/tmp/index"),
      ),
    )

    assertThatThrownBy {
      iut.putVectors(
        indexName = "index",
        vectorBucketName = "bucket",
        indexArn = null,
        vectors =
          listOf(
            PutInputVector(
              key = "k1",
              data = VectorData(float32 = listOf(1.0f, 2.0f)),
            ),
          ),
      )
    }.isSameAs(S3Exception.VECTOR_VALIDATION)
  }
}
