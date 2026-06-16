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
package com.adobe.testing.s3mock.vectors.store

import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.store.ObjectStore
import com.adobe.testing.s3mock.store.StoreConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.io.File

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@ActiveProfiles("vectors")
@MockitoBean(types = [KmsKeyStore::class, ObjectStore::class, MultipartStore::class])
@SpringBootTest(
  classes = [StoreConfiguration::class, VectorsStoreConfiguration::class],
  webEnvironment = SpringBootTest.WebEnvironment.NONE,
)
internal class VectorIndexStoreTest {
  @Autowired
  private lateinit var iut: VectorIndexStore

  @Autowired
  private lateinit var vectorBucketStore: VectorBucketStore

  @Autowired
  private lateinit var rootFolder: File

  private val bucket = "test-bucket"

  @BeforeEach
  fun setup() {
    vectorBucketStore.createVectorBucket(bucket, null, null, emptyMap())
  }

  @AfterEach
  fun cleanup() {
    rootFolder.resolve("vectors").deleteRecursively()
  }

  @Test
  fun `creates an index and it exists afterwards`() {
    iut.createIndex(bucket, "my-index", "float32", 128, "cosine", null, null, emptyList(), emptyMap())

    assertThat(iut.doesIndexExist(bucket, "my-index")).isTrue()
  }

  @Test
  fun `doesIndexExist returns false for non-existing index`() {
    assertThat(iut.doesIndexExist(bucket, "no-index")).isFalse()
  }

  @Test
  fun `getIndexMetadata returns stored metadata`() {
    iut.createIndex(
      bucket,
      "meta-index",
      "float32",
      256,
      "euclidean",
      "AES256",
      null,
      listOf("internal"),
      mapOf("env" to "test"),
    )

    val meta = iut.getIndexMetadata(bucket, "meta-index")

    assertThat(meta.name).isEqualTo("meta-index")
    assertThat(meta.dimension).isEqualTo(256)
    assertThat(meta.distanceMetric).isEqualTo("euclidean")
    assertThat(meta.sseType).isEqualTo("AES256")
    assertThat(meta.nonFilterableMetadataKeys).containsExactly("internal")
    assertThat(meta.tags).containsEntry("env", "test")
  }

  @Test
  fun `listIndexes returns indexes sorted by name`() {
    iut.createIndex(bucket, "idx-b", "float32", 4, "cosine", null, null, emptyList(), emptyMap())
    iut.createIndex(bucket, "idx-a", "float32", 4, "cosine", null, null, emptyList(), emptyMap())

    val indexes = iut.listIndexes(bucket)

    assertThat(indexes.map { it.name }).containsExactly("idx-a", "idx-b")
  }

  @Test
  fun `listIndexes returns empty list for bucket with no indexes`() {
    assertThat(iut.listIndexes(bucket)).isEmpty()
  }

  @Test
  fun `deleteIndex removes the index`() {
    iut.createIndex(bucket, "del-index", "float32", 4, "cosine", null, null, emptyList(), emptyMap())
    iut.deleteIndex(bucket, "del-index")

    assertThat(iut.doesIndexExist(bucket, "del-index")).isFalse()
  }

  @Test
  fun `updateTags merges tags`() {
    iut.createIndex(bucket, "tag-index", "float32", 4, "cosine", null, null, emptyList(), mapOf("a" to "1"))
    iut.updateTags(bucket, "tag-index", mapOf("b" to "2"))

    val meta = iut.getIndexMetadata(bucket, "tag-index")
    assertThat(meta.tags).containsEntry("a", "1").containsEntry("b", "2")
  }

  @Test
  fun `removeTags removes specified keys`() {
    iut.createIndex(bucket, "untag-idx", "float32", 4, "cosine", null, null, emptyList(), mapOf("a" to "1", "b" to "2"))
    iut.removeTags(bucket, "untag-idx", listOf("a"))

    val meta = iut.getIndexMetadata(bucket, "untag-idx")
    assertThat(meta.tags).doesNotContainKey("a").containsEntry("b", "2")
  }

  @Test
  fun `hasIndexes in bucket returns true after creating an index`() {
    iut.createIndex(bucket, "exists-idx", "float32", 4, "cosine", null, null, emptyList(), emptyMap())

    assertThat(vectorBucketStore.hasIndexes(bucket)).isTrue()
  }
}
