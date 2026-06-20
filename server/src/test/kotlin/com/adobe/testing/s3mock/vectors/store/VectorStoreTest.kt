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
internal class VectorStoreTest {
  @Autowired
  private lateinit var iut: VectorStore

  @Autowired
  private lateinit var vectorBucketStore: VectorBucketStore

  @Autowired
  private lateinit var vectorIndexStore: VectorIndexStore

  @Autowired
  private lateinit var rootFolder: File

  private val bucket = "test-bucket"
  private val index = "test-index"

  @BeforeEach
  fun setup() {
    vectorBucketStore.createVectorBucket(bucket, null, null, emptyMap())
    vectorIndexStore.createIndex(bucket, index, "float32", 3, "cosine", null, null, emptyList(), emptyMap())
  }

  @AfterEach
  fun cleanup() {
    rootFolder.resolve("vectors").deleteRecursively()
  }

  @Test
  fun `putVector and getVector round-trip preserves float data`() {
    val floats = floatArrayOf(0.1f, 0.2f, 0.3f)
    iut.putVector(bucket, index, "my-key", floats, null)

    val stored = iut.getVector(bucket, index, "my-key", returnData = true, returnMetadata = false)

    assertThat(stored).isNotNull
    assertThat(stored!!.key).isEqualTo("my-key")
    assertThat(stored.floats).hasSize(3)
    assertThat(stored.floats!![0]).isEqualTo(0.1f)
    assertThat(stored.floats!![1]).isEqualTo(0.2f)
    assertThat(stored.floats!![2]).isEqualTo(0.3f)
  }

  @Test
  fun `putVector stores metadata and getVector retrieves it`() {
    iut.putVector(bucket, index, "meta-key", floatArrayOf(1f, 0f, 0f), mapOf("genre" to "drama"))

    val stored = iut.getVector(bucket, index, "meta-key", returnData = false, returnMetadata = true)

    assertThat(stored).isNotNull
    assertThat(stored!!.metadata).isNotNull
    assertThat(stored.metadata!!.get("genre").textValue()).isEqualTo("drama")
  }

  @Test
  fun `getVector returns null for non-existing key`() {
    val stored = iut.getVector(bucket, index, "no-such-key", returnData = false, returnMetadata = false)

    assertThat(stored).isNull()
  }

  @Test
  fun `getVector with returnData=false returns null floats`() {
    iut.putVector(bucket, index, "no-data-key", floatArrayOf(1f, 0f, 0f), null)

    val stored = iut.getVector(bucket, index, "no-data-key", returnData = false, returnMetadata = false)

    assertThat(stored).isNotNull
    assertThat(stored!!.floats).isNull()
  }

  @Test
  fun `vectorExists returns true after putVector`() {
    iut.putVector(bucket, index, "exists-key", floatArrayOf(1f, 0f, 0f), null)

    assertThat(iut.vectorExists(bucket, index, "exists-key")).isTrue()
  }

  @Test
  fun `vectorExists returns false before putVector`() {
    assertThat(iut.vectorExists(bucket, index, "not-yet")).isFalse()
  }

  @Test
  fun `deleteVector removes the vector`() {
    iut.putVector(bucket, index, "del-key", floatArrayOf(1f, 0f, 0f), null)
    iut.deleteVector(bucket, index, "del-key")

    assertThat(iut.vectorExists(bucket, index, "del-key")).isFalse()
  }

  @Test
  fun `listVectors returns all stored vectors sorted by key`() {
    iut.putVector(bucket, index, "k-b", floatArrayOf(1f, 0f, 0f), null)
    iut.putVector(bucket, index, "k-a", floatArrayOf(0f, 1f, 0f), null)

    val listed = iut.listVectors(bucket, index, returnData = false, returnMetadata = false)

    assertThat(listed.map { it.key }).containsExactly("k-a", "k-b")
  }

  @Test
  fun `listVectors returns empty for empty index`() {
    assertThat(iut.listVectors(bucket, index, returnData = false, returnMetadata = false)).isEmpty()
  }

  @Test
  fun `putVector with unicode key round-trips the key correctly`() {
    val unicodeKey = "héllo/wörld=key"
    iut.putVector(bucket, index, unicodeKey, floatArrayOf(1f, 0f, 0f), null)

    val stored = iut.getVector(bucket, index, unicodeKey, returnData = false, returnMetadata = false)

    assertThat(stored).isNotNull
    assertThat(stored!!.key).isEqualTo(unicodeKey)
  }

  @Test
  fun `float byte encoding round-trips correctly`() {
    val floats = floatArrayOf(Float.MAX_VALUE, Float.MIN_VALUE, -1f, 0f, 1f)
    val bytes = VectorStore.floatsToBytes(floats)
    val recovered = VectorStore.bytesToFloats(bytes)

    assertThat(recovered).isEqualTo(floats)
  }

  @Test
  fun `sha256Hex produces consistent results for the same input`() {
    val h1 = VectorStore.sha256Hex("test-key")
    val h2 = VectorStore.sha256Hex("test-key")

    assertThat(h1).isEqualTo(h2).hasSize(64)
  }

  @Test
  fun `sha256Hex produces different results for different inputs`() {
    assertThat(VectorStore.sha256Hex("key-a")).isNotEqualTo(VectorStore.sha256Hex("key-b"))
  }

  @Test
  fun `putVector is upsert and re-putting the same key overwrites`() {
    iut.putVector(bucket, index, "upsert-key", floatArrayOf(1f, 0f, 0f), null)
    iut.putVector(bucket, index, "upsert-key", floatArrayOf(0f, 1f, 0f), null)

    val stored = iut.getVector(bucket, index, "upsert-key", returnData = true, returnMetadata = false)
    assertThat(stored!!.floats!![1]).isEqualTo(1f)
  }
}
