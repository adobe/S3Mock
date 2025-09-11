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

import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.BucketType
import com.adobe.testing.s3mock.dto.DataRedundancy
import com.adobe.testing.s3mock.dto.LifecycleRule
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.LocationType
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.ObjectOwnership.BUCKET_OWNER_ENFORCED
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Transition
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockitoBean(types = [KmsKeyStore::class, ObjectStore::class, MultipartStore::class])
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
internal class BucketStoreTest : StoreTestBase() {
  @Autowired
  private lateinit var bucketStore: BucketStore

  @Test
  fun `creates a bucket with expected name and path`() {
    val bucket = givenBucket()

    assertThat(bucket.name).endsWith(TEST_BUCKET_NAME)
    assertThat(bucket.path).exists()
  }

  @Test
  fun `doesBucketExist returns true for existing bucket`() {
    givenBucket()

    val doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME)

    assertThat(doesBucketExist).isTrue()
  }

  @Test
  fun `doesBucketExist returns false for non-existing bucket`() {
    val doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME)

    assertThat(doesBucketExist).isFalse()
  }

  @Test
  fun `creates and lists buckets with umlauts`() {
    val bucketName1 = "myNüwNämeÄins"
    val bucketName2 = "myNüwNämeZwöei"
    val bucketName3 = "myNüwNämeDrü"

    givenBucket(bucketName1)
    givenBucket(bucketName2)
    givenBucket(bucketName3)

    val buckets = bucketStore.listBuckets()

    assertThat(buckets).`as`("FileStore should hold three Buckets").hasSize(3)
  }

  @Test
  fun `creates and gets a bucket`() {
    givenBucket()

    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucket).isNotNull()
    assertThat(bucket.name).isEqualTo(TEST_BUCKET_NAME)
  }

  @Test
  fun `creates and gets bucket with object lock`() {
    givenBucket(
      objectLockEnabled = true,
    )

    val bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    assertThat(bucket).isNotNull()
    assertThat(bucket.name).isEqualTo(TEST_BUCKET_NAME)
    assertThat(bucket.objectLockConfiguration).isNotNull()
    assertThat(bucket.objectLockConfiguration?.objectLockRule).isNull()
    assertThat(bucket.objectLockConfiguration?.objectLockEnabled).isEqualTo(ObjectLockEnabled.ENABLED)
  }

  @Test
  fun `stores and retrieves bucket lifecycle configuration`() {
    givenBucket()

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
  fun `deletes empty bucket successfully`() {
    givenBucket()

    val bucketDeleted = bucketStore.deleteBucket(TEST_BUCKET_NAME)
    val bucket = bucketStore.doesBucketExist(TEST_BUCKET_NAME)

    assertThat(bucketDeleted).isTrue()
    assertThat(bucket).isFalse()
  }

  @Test
  fun `isBucketEmpty is true for new bucket and false after adding a key`() {
    givenBucket()

    // Newly created bucket should be empty
    assertThat(bucketStore.isBucketEmpty(TEST_BUCKET_NAME)).isTrue()

    // Add a key and verify bucket is no longer empty
    bucketStore.addKeyToBucket("folder/file.txt", TEST_BUCKET_NAME)
    assertThat(bucketStore.isBucketEmpty(TEST_BUCKET_NAME)).isFalse()
  }

  @Test
  fun `add, lookup and remove keys with and without prefix`() {
    givenBucket()

    val id1 = bucketStore.addKeyToBucket("a/1.txt", TEST_BUCKET_NAME)
    val id2 = bucketStore.addKeyToBucket("a/2.txt", TEST_BUCKET_NAME)
    val id3 = bucketStore.addKeyToBucket("b/1.txt", TEST_BUCKET_NAME)

    // lookup with null prefix -> all keys
    val all = bucketStore.lookupIdsInBucket(null, TEST_BUCKET_NAME)
    assertThat(all).containsExactlyInAnyOrder(id1, id2, id3)

    // lookup for prefix "a/" -> two keys
    val aOnly = bucketStore.lookupIdsInBucket("a/", TEST_BUCKET_NAME)
    assertThat(aOnly).containsExactlyInAnyOrder(id1, id2)

    // lookup for non-matching prefix
    val none = bucketStore.lookupIdsInBucket("c/", TEST_BUCKET_NAME)
    assertThat(none).isEmpty()

    // remove key and verify behavior
    val removed = bucketStore.removeFromBucket("a/1.txt", TEST_BUCKET_NAME)
    assertThat(removed).isTrue()
    val removedAgain = bucketStore.removeFromBucket("a/1.txt", TEST_BUCKET_NAME)
    assertThat(removedAgain).isFalse()
  }

  @Test
  fun `deleteBucket returns false and does not delete a non-empty bucket`() {
    givenBucket()

    bucketStore.addKeyToBucket("keep/me.txt", TEST_BUCKET_NAME)
    val deleted = bucketStore.deleteBucket(TEST_BUCKET_NAME)

    assertThat(deleted).isFalse()
    assertThat(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).isTrue()
  }

  @Test
  fun `object lock disabled by default and enabled after storing configuration`() {
    // Create without object lock -> disabled
    givenBucket()

    assertThat(bucketStore.isObjectLockEnabled(TEST_BUCKET_NAME)).isFalse()

    // Store configuration with ENABLED and verify
    val meta = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)
    bucketStore.storeObjectLockConfiguration(
      meta,
      ObjectLockConfiguration(ObjectLockEnabled.ENABLED, null)
    )
    assertThat(bucketStore.isObjectLockEnabled(TEST_BUCKET_NAME)).isTrue()
  }

  @Test
  fun `versioning flags reflect enabled and suspended states`() {
    givenBucket()

    var meta = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)

    // enable versioning
    bucketStore.storeVersioningConfiguration(
      meta,
      VersioningConfiguration(null, VersioningConfiguration.Status.ENABLED)
    )
    meta = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)
    assertThat(meta.isVersioningEnabled).isTrue()
    assertThat(meta.isVersioningSuspended).isFalse()

    // suspend versioning
    bucketStore.storeVersioningConfiguration(
      meta,
      VersioningConfiguration(null, VersioningConfiguration.Status.SUSPENDED)
    )
    meta = bucketStore.getBucketMetadata(TEST_BUCKET_NAME)
    assertThat(meta.isVersioningEnabled).isFalse()
    assertThat(meta.isVersioningSuspended).isTrue()
  }

  @Test
  fun `creates bucket with custom region, bucket info and location info`() {
    val region = "eu-west-1"
    val bucketInfo = BucketInfo(DataRedundancy.SINGLE_AVAILABILITY_ZONE, BucketType.DIRECTORY)
    val locationInfo = LocationInfo("eu-west-1a", LocationType.AVAILABILITY_ZONE)

    val bucket = givenBucket(
      region = region,
      bucketInfo = bucketInfo,
      locationInfo = locationInfo,
    )

    assertThat(bucket.bucketRegion).isEqualTo(region)
    assertThat(bucket.objectOwnership).isEqualTo(BUCKET_OWNER_ENFORCED)
    assertThat(bucket.bucketInfo).isEqualTo(bucketInfo)
    assertThat(bucket.locationInfo).isEqualTo(locationInfo)
  }

  @Test
  fun `isBucketEmpty throws for non-existing bucket`() {
    assertThatThrownBy {
      bucketStore.isBucketEmpty("does-not-exist")
    }.isInstanceOf(IllegalStateException::class.java)
  }

  @Test
  fun `loadBuckets returns existing object ids`() {
    givenBucket()

    val id1 = bucketStore.addKeyToBucket("x/1", TEST_BUCKET_NAME)
    val id2 = bucketStore.addKeyToBucket("x/2", TEST_BUCKET_NAME)

    val loaded = bucketStore.loadBuckets(listOf(TEST_BUCKET_NAME))
    assertThat(loaded).contains(id1, id2)
  }

  fun givenBucket(
    bucketName: String = TEST_BUCKET_NAME,
    objectLockEnabled: Boolean = false,
    objectOwnership: ObjectOwnership = BUCKET_OWNER_ENFORCED,
    region: String = "us-east-1",
    bucketInfo: BucketInfo? = null,
    locationInfo: LocationInfo? = null,
  ) = bucketStore.createBucket(
    bucketName,
    objectLockEnabled,
    objectOwnership,
    region,
    bucketInfo,
    locationInfo
  )

  /**
   * Delete all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    bucketStore.listBuckets().forEach { bucket ->
      bucketStore.lookupKeysInBucket(null, bucket.name).forEach { key ->
        bucketStore.removeFromBucket(key, bucket.name)
      }
      bucketStore.deleteBucket(bucket.name)
    }
  }
}
