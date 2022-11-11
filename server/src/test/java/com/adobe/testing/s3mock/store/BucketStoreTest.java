/*
 *  Copyright 2017-2022 Adobe.
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
import java.util.List;
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
@SpringBootTest(classes = {StoreConfiguration.class})
class BucketStoreTest extends StoreTestBase {
  @Autowired
  private BucketStore bucketStore;

  @Test
  void testCreateBucket() {
    BucketMetadata bucket = bucketStore.createBucket(TEST_BUCKET_NAME, false);
    assertThat(bucket.getName()).as("Bucket should have been created.").endsWith(TEST_BUCKET_NAME);
    assertThat(bucket.getPath()).exists();
  }

  @Test
  void testDoesBucketExist_ok() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);

    Boolean doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
            String.format("The previously created bucket, '%s', should exist!", TEST_BUCKET_NAME))
        .isTrue();
  }

  @Test
  void testDoesBucketExist_nonExistingBucket() {
    Boolean doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
        String.format("The bucket, '%s', should not exist!", TEST_BUCKET_NAME)).isFalse();
  }

  @Test
  void testCreateAndListBucketsWithUmlauts() {
    String bucketName1 = "myNüwNämeÄins";
    String bucketName2 = "myNüwNämeZwöei";
    String bucketName3 = "myNüwNämeDrü";

    bucketStore.createBucket(bucketName1, false);
    bucketStore.createBucket(bucketName2, false);
    bucketStore.createBucket(bucketName3, false);

    List<BucketMetadata> buckets = bucketStore.listBuckets();

    assertThat(buckets.size()).as("FileStore should hold three Buckets").isEqualTo(3);
  }

  @Test
  void testCreateAndGetBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket).as("Bucket should not be null").isNotNull();
    assertThat(bucket.getName()).as("Bucket name should end with " + TEST_BUCKET_NAME)
        .isEqualTo(TEST_BUCKET_NAME);
  }

  @Test
  void testCreateAndGetBucketWithObjectLock() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket).as("Bucket should not be null").isNotNull();
    assertThat(bucket.getName()).as("Bucket name should end with " + TEST_BUCKET_NAME)
        .isEqualTo(TEST_BUCKET_NAME);
    assertThat(bucket.getObjectLockConfiguration()).isNotNull();
    assertThat(bucket.getObjectLockConfiguration().getObjectLockRule()).isNull();
    assertThat(bucket.getObjectLockConfiguration().getObjectLockEnabled()).isEqualTo(ENABLED);
  }

  @Test
  void testStoreAndGetBucketLifecycleConfiguration() {
    bucketStore.createBucket(TEST_BUCKET_NAME, true);

    LifecycleRuleFilter filter1 = new LifecycleRuleFilter(null, null, "documents/", null, null);
    Transition transition1 = new Transition(null, 30, GLACIER);
    LifecycleRule rule1 = new LifecycleRule(null, null, filter1, "id1", null, null,
        LifecycleRule.Status.ENABLED, singletonList(transition1));
    BucketLifecycleConfiguration configuration =
        new BucketLifecycleConfiguration(singletonList(rule1));

    bucketStore.storeBucketLifecycleConfiguration(TEST_BUCKET_NAME, configuration);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket.getBucketLifecycleConfiguration()).isEqualTo(configuration);
  }

  @Test
  void testCreateAndDeleteBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);
    boolean bucketDeleted = bucketStore.deleteBucket(TEST_BUCKET_NAME);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucketDeleted).as("Deletion should succeed!").isTrue();
    assertThat(bucket).as("Bucket should be null!").isNull();
  }

  /**
   * Delete all existing buckets.
   */
  @AfterEach
  void cleanupStores() {
    for (BucketMetadata bucket : bucketStore.listBuckets()) {
      bucketStore.deleteBucket(bucket.getName());
    }
  }
}
