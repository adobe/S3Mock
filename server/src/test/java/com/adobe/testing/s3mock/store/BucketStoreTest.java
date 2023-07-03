/*
 *  Copyright 2017-2023 Adobe.
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

package com.adobe.testing.s3mock.store;

import static com.adobe.testing.s3mock.dto.ObjectLockEnabled.ENABLED;
import static com.adobe.testing.s3mock.dto.StorageClass.GLACIER;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.LifecycleRule;
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter;
import com.adobe.testing.s3mock.dto.Transition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = {KmsKeyStore.class, ObjectStore.class, MultipartStore.class})
@SpringBootTest(classes = {StoreConfiguration.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
class BucketStoreTest extends StoreTestBase {
  @Autowired
  private BucketStore bucketStore;

  @Test
  void testCreateBucket() {
    var bucket = bucketStore.createBucket(TEST_BUCKET_NAME, false);
    assertThat(bucket.name()).as("Bucket should have been created.").endsWith(TEST_BUCKET_NAME);
    assertThat(bucket.path()).exists();
  }

  @Test
  void testDoesBucketExist_ok() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);

    var doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
            String.format("The previously created bucket, '%s', should exist!", TEST_BUCKET_NAME))
        .isTrue();
  }

  @Test
  void testDoesBucketExist_nonExistingBucket() {
    var doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
        String.format("The bucket, '%s', should not exist!", TEST_BUCKET_NAME)).isFalse();
  }

  @Test
  void testCreateAndListBucketsWithUmlauts() {
    var bucketName1 = "myNüwNämeÄins";
    var bucketName2 = "myNüwNämeZwöei";
    var bucketName3 = "myNüwNämeDrü";

    bucketStore.createBucket(bucketName1, false);
    bucketStore.createBucket(bucketName2, false);
    bucketStore.createBucket(bucketName3, false);

    var buckets = bucketStore.listBuckets();

    assertThat(buckets).as("FileStore should hold three Buckets").hasSize(3);
  }

  @Test
  void testCreateAndGetBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);
    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket).as("Bucket should not be null").isNotNull();
    assertThat(bucket.name()).as("Bucket name should end with " + TEST_BUCKET_NAME)
        .isEqualTo(TEST_BUCKET_NAME);
  }

  @Test
  void testCreateAndGetBucketWithObjectLock() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true);
    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket).as("Bucket should not be null").isNotNull();
    assertThat(bucket.name()).as("Bucket name should end with " + TEST_BUCKET_NAME)
        .isEqualTo(TEST_BUCKET_NAME);
    assertThat(bucket.objectLockConfiguration()).isNotNull();
    assertThat(bucket.objectLockConfiguration().objectLockRule()).isNull();
    assertThat(bucket.objectLockConfiguration().objectLockEnabled()).isEqualTo(ENABLED);
  }

  @Test
  void testStoreAndGetBucketLifecycleConfiguration() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true);

    var filter1 = new LifecycleRuleFilter(null, null, "documents/", null, null);
    var transition1 = new Transition(null, 30, GLACIER);
    var rule1 = new LifecycleRule(null, null, filter1, "id1", null, null,
        LifecycleRule.Status.ENABLED, singletonList(transition1));
    var configuration = new BucketLifecycleConfiguration(singletonList(rule1));

    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);
    bucketStore.storeBucketLifecycleConfiguration(bucket, configuration);
    bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket.bucketLifecycleConfiguration()).isEqualTo(configuration);
  }

  @Test
  void testCreateAndDeleteBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);
    var bucketDeleted = bucketStore.deleteBucket(TEST_BUCKET_NAME);
    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucketDeleted).as("Deletion should succeed!").isTrue();
    assertThat(bucket).as("Bucket should be null!").isNull();
  }

  /**
   * Delete all existing buckets.
   */
  @AfterEach
  void cleanupStores() {
    for (BucketMetadata bucket : bucketStore.listBuckets()) {
      bucketStore.deleteBucket(bucket.name());
    }
  }
}
