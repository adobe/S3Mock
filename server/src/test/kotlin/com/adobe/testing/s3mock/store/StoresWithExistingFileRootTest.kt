/*
 *  Copyright 2017-2024 Adobe.
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

import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.StoresWithExistingFileRootTest.TestConfig
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.AssertionsForClassTypes.assertThat
import org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.context.annotation.Bean
import java.io.File
import java.util.UUID

@MockBean(classes = [KmsKeyStore::class])
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
    bucketStore.createBucket(TEST_BUCKET_NAME, false)
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
    objectStore
      .storeS3ObjectMetadata(
        bucketMetadata, id, name, TEXT_PLAIN, storeHeaders(), path,
        emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD
      )

    assertThatThrownBy { testObjectStore.getS3ObjectMetadata(bucketMetadata, id) }
      .isInstanceOf(
        NullPointerException::class.java
      )

    objectStore.getS3ObjectMetadata(bucketMetadata, id).also {
      testObjectStore.loadObjects(bucketMetadata, listOf(it.id))

      testObjectStore.getS3ObjectMetadata(bucketMetadata, id).also {
        assertThat(it.modificationDate).isEqualTo(it.modificationDate)
        assertThat(it.etag).isEqualTo(it.etag)
      }
    }


  }

  @TestConfiguration
  open class TestConfig {
    @Bean
    open fun testBucketStore(
      properties: StoreProperties, rootFolder: File?,
      objectMapper: ObjectMapper?
    ): BucketStore {
      return BucketStore(
        rootFolder, properties.retainFilesOnExit,
        StoreConfiguration.S3_OBJECT_DATE_FORMAT, objectMapper
      )
    }

    @Bean
    open fun testObjectStore(properties: StoreProperties, objectMapper: ObjectMapper?): ObjectStore {
      return ObjectStore(
        properties.retainFilesOnExit,
        StoreConfiguration.S3_OBJECT_DATE_FORMAT, objectMapper
      )
    }

    @Bean
    open fun objectMapper(): ObjectMapper {
      return ObjectMapper()
    }
  }
}
