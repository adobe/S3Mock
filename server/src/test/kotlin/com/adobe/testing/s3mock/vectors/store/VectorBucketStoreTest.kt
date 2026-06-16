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
internal class VectorBucketStoreTest {
  @Autowired
  private lateinit var iut: VectorBucketStore

  @Autowired
  private lateinit var rootFolder: File

  @AfterEach
  fun cleanup() {
    rootFolder.resolve("vectors").deleteRecursively()
  }

  @Test
  fun `creates a bucket and it exists afterwards`() {
    iut.createVectorBucket("my-bucket", null, null, emptyMap())

    assertThat(iut.doesBucketExist("my-bucket")).isTrue()
  }

  @Test
  fun `doesBucketExist returns false for non-existing bucket`() {
    assertThat(iut.doesBucketExist("no-such-bucket")).isFalse()
  }

  @Test
  fun `getBucketMetadata returns stored metadata`() {
    iut.createVectorBucket("meta-bucket", "AES256", null, mapOf("env" to "test"))

    val meta = iut.getBucketMetadata("meta-bucket")

    assertThat(meta.name).isEqualTo("meta-bucket")
    assertThat(meta.sseType).isEqualTo("AES256")
    assertThat(meta.tags).containsEntry("env", "test")
  }

  @Test
  fun `listBuckets returns all created buckets sorted by name`() {
    iut.createVectorBucket("bucket-b", null, null, emptyMap())
    iut.createVectorBucket("bucket-a", null, null, emptyMap())

    val buckets = iut.listBuckets()

    assertThat(buckets.map { it.name }).containsExactly("bucket-a", "bucket-b")
  }

  @Test
  fun `listBuckets returns empty list when no buckets exist`() {
    assertThat(iut.listBuckets()).isEmpty()
  }

  @Test
  fun `deleteBucket removes the bucket`() {
    iut.createVectorBucket("del-bucket", null, null, emptyMap())
    iut.deleteBucket("del-bucket")

    assertThat(iut.doesBucketExist("del-bucket")).isFalse()
  }

  @Test
  fun `updateTags merges tags into existing tags`() {
    iut.createVectorBucket("tag-bucket", null, null, mapOf("a" to "1"))
    iut.updateTags("tag-bucket", mapOf("b" to "2"))

    val meta = iut.getBucketMetadata("tag-bucket")
    assertThat(meta.tags).containsEntry("a", "1").containsEntry("b", "2")
  }

  @Test
  fun `removeTags removes specified keys`() {
    iut.createVectorBucket("untag-bucket", null, null, mapOf("a" to "1", "b" to "2"))
    iut.removeTags("untag-bucket", listOf("a"))

    val meta = iut.getBucketMetadata("untag-bucket")
    assertThat(meta.tags).doesNotContainKey("a").containsEntry("b", "2")
  }

  @Test
  fun `storePolicy and getPolicy round-trip`() {
    iut.createVectorBucket("policy-bucket", null, null, emptyMap())
    iut.storePolicy("policy-bucket", """{"Version":"2012-10-17"}""")

    assertThat(iut.getPolicy("policy-bucket")).isEqualTo("""{"Version":"2012-10-17"}""")
  }

  @Test
  fun `getPolicy returns null when no policy is stored`() {
    iut.createVectorBucket("no-policy-bucket", null, null, emptyMap())

    assertThat(iut.getPolicy("no-policy-bucket")).isNull()
  }

  @Test
  fun `deletePolicy removes the stored policy`() {
    iut.createVectorBucket("dp-bucket", null, null, emptyMap())
    iut.storePolicy("dp-bucket", "{}")
    iut.deletePolicy("dp-bucket")

    assertThat(iut.getPolicy("dp-bucket")).isNull()
  }

  @Test
  fun `hasIndexes returns false for fresh bucket`() {
    iut.createVectorBucket("idx-check-bucket", null, null, emptyMap())

    assertThat(iut.hasIndexes("idx-check-bucket")).isFalse()
  }
}
