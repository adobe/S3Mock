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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.StoresWithExistingFileRootTest.TestConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.assertj.core.api.AssertionsForClassTypes.assertThat
import org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.context.bean.override.mockito.MockitoBean
import software.amazon.awssdk.regions.Region
import java.io.File
import java.util.UUID

@MockitoBean(types = [KmsKeyStore::class])
@SpringBootTest(
  classes = [StoreConfiguration::class, TestConfig::class], webEnvironment = SpringBootTest.WebEnvironment.NONE
)
internal class StoresWithExistingFileRootTest : StoreTestBase() {
  @Autowired
  private lateinit var bucketStore: BucketStore

  @Autowired
  private lateinit var testBucketStore: BucketStore

  @Autowired
  private lateinit var objectStore: ObjectStore

  @Autowired
  private lateinit var testObjectStore: ObjectStore

  @Test
  fun testBucketStoreWithExistingRoot() {
    bucketStore.createBucket(
      TEST_BUCKET_NAME,
      false,
      ObjectOwnership.BUCKET_OWNER_ENFORCED,
      "us-east-1",
      null,
      null
    )
    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThatThrownBy { testBucketStore.getBucketMetadata(TEST_BUCKET_NAME) }
      .isInstanceOf(NullPointerException::class.java)

    testBucketStore.loadBuckets(listOf(TEST_BUCKET_NAME))
    testBucketStore.getBucketMetadata(TEST_BUCKET_NAME).also {
      assertThat(it.creationDate).isEqualTo(bucket.creationDate)
      assertThat(it.path).isEqualTo(bucket.path)
    }
  }

  @Test
  fun testObjectStoreWithExistingRoot() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = UUID.randomUUID()
    val name = sourceFile.name
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)

    objectStore.storeS3ObjectMetadata(
      bucketMetadata,
      id,
      name,
      TEXT_PLAIN,
      storeHeaders(),
      path,
      emptyMap(),
      emptyMap(),
      null,
      emptyList(),
      null,
      null,
      Owner.DEFAULT_OWNER,
      StorageClass.STANDARD,
      ChecksumType.FULL_OBJECT
    )

    assertThatThrownBy { testObjectStore.getS3ObjectMetadata(bucketMetadata, id, null) }
      .isInstanceOf(NullPointerException::class.java)

    val originalMeta = objectStore.getS3ObjectMetadata(bucketMetadata, id, null)!!
    testObjectStore.loadObjects(bucketMetadata, listOf(originalMeta.id))

    val reloadedMeta = testObjectStore.getS3ObjectMetadata(bucketMetadata, id, null)!!
    assertThat(reloadedMeta.modificationDate).isEqualTo(originalMeta.modificationDate)
    assertThat(reloadedMeta.etag).isEqualTo(originalMeta.etag)
  }

  @TestConfiguration
  open class TestConfig {
    @Bean
    open fun testBucketStore(
      rootFolder: File,
      objectMapper: ObjectMapper
    ): BucketStore = BucketStore(
      rootFolder,
      StoreConfiguration.S3_OBJECT_DATE_FORMAT,
      Region.EU_CENTRAL_1.id(),
      objectMapper
    )

    @Bean
    open fun testObjectStore(objectMapper: ObjectMapper): ObjectStore =
      ObjectStore(StoreConfiguration.S3_OBJECT_DATE_FORMAT, objectMapper)

    @Bean
    open fun objectMapper(): ObjectMapper = ObjectMapper().registerKotlinModule()
  }
}
