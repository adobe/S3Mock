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

import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.LifecycleRule
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Transition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import software.amazon.awssdk.services.s3.model.ObjectOwnership.BUCKET_OWNER_ENFORCED

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = [KmsKeyStore::class, ObjectStore::class, MultipartStore::class])
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
internal class BucketStoreTest : StoreTestBase() {
  @Autowired
  private lateinit var bucketStore: BucketStore

  @Test
  fun testCreateBucket() {
    val bucket = bucketStore.createBucket(TEST_BUCKET_NAME, false,
      BUCKET_OWNER_ENFORCED)
    assertThat(bucket.name).endsWith(TEST_BUCKET_NAME)
    assertThat(bucket.path).exists()
  }

  @Test
  fun testDoesBucketExist_ok() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED)

    val doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME)

    assertThat(doesBucketExist).isTrue()
  }

  @Test
  fun testDoesBucketExist_nonExistingBucket() {
    val doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME)

    assertThat(doesBucketExist).isFalse()
  }

  @Test
  fun testCreateAndListBucketsWithUmlauts() {
    val bucketName1 = "myNüwNämeÄins"
    val bucketName2 = "myNüwNämeZwöei"
    val bucketName3 = "myNüwNämeDrü"

    bucketStore.createBucket(bucketName1, false, BUCKET_OWNER_ENFORCED)
    bucketStore.createBucket(bucketName2, false, BUCKET_OWNER_ENFORCED)
    bucketStore.createBucket(bucketName3, false, BUCKET_OWNER_ENFORCED)

    val buckets = bucketStore.listBuckets()

    assertThat(buckets).`as`("FileStore should hold three Buckets").hasSize(3)
  }

  @Test
  fun testCreateAndGetBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED)
    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucket).isNotNull()
    assertThat(bucket.name).isEqualTo(TEST_BUCKET_NAME)
  }

  @Test
  fun testCreateAndGetBucketWithObjectLock() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true, BUCKET_OWNER_ENFORCED)
    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucket).isNotNull()
    assertThat(bucket.name).isEqualTo(TEST_BUCKET_NAME)
    assertThat(bucket.objectLockConfiguration).isNotNull()
    assertThat(bucket.objectLockConfiguration.objectLockRule).isNull()
    assertThat(bucket.objectLockConfiguration.objectLockEnabled).isEqualTo(ObjectLockEnabled.ENABLED)
  }

  @Test
  fun testStoreAndGetBucketLifecycleConfiguration() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true, BUCKET_OWNER_ENFORCED)

    val filter1 = LifecycleRuleFilter(null, null, "documents/", null, null)
    val transition1 = Transition(null, 30, StorageClass.GLACIER)
    val rule1 = LifecycleRule(
      null, null, filter1, "id1", null, null,
      LifecycleRule.Status.ENABLED, listOf(transition1)
    )
    val configuration = BucketLifecycleConfiguration(listOf(rule1))

    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)
    bucketStore.storeBucketLifecycleConfiguration(bucket, configuration)
    bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucket.bucketLifecycleConfiguration).isEqualTo(configuration)
  }

  @Test
  fun testCreateAndDeleteBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false, BUCKET_OWNER_ENFORCED)
    val bucketDeleted = bucketStore.deleteBucket(TEST_BUCKET_NAME)
    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucketDeleted).isTrue()
    assertThat(bucket).isNull()
  }

  /**
   * Delete all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    for (bucket in bucketStore.listBuckets()) {
      bucketStore.deleteBucket(bucket.name)
    }
  }
}
