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

import static org.assertj.core.api.Assertions.assertThat;

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
class BucketStoreTest {

  private static final String TEST_BUCKET_NAME = "test-bucket";

  @Autowired
  private BucketStore bucketStore;

  /**
   * Creates a bucket and checks that it exists.
   *
   */
  @Test
  void shouldCreateBucket() {
    final BucketMetadata bucket = bucketStore.createBucket(TEST_BUCKET_NAME);
    assertThat(bucket.getName()).as("Bucket should have been created.").endsWith(TEST_BUCKET_NAME);
    assertThat(bucket.getPath()).exists();
  }

  /**
   * Checks if Bucket exists.
   *
   */
  @Test
  void bucketShouldExist() {
    bucketStore.createBucket(TEST_BUCKET_NAME);

    final Boolean doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
            String.format("The previously created bucket, '%s', should exist!", TEST_BUCKET_NAME))
        .isTrue();
  }

  /**
   * Checks if bucket doesn't exist.
   */
  @Test
  void bucketShouldNotExist() {
    final Boolean doesBucketExist = bucketStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(doesBucketExist).as(
        String.format("The bucket, '%s', should not exist!", TEST_BUCKET_NAME)).isFalse();
  }

  /**
   * Checks if created buckets with weird names are listed.
   */
  @Test
  void shouldHoldAllBuckets() {
    final String bucketName1 = "myNüwNämeÄins";
    final String bucketName2 = "myNüwNämeZwöei";
    final String bucketName3 = "myNüwNämeDrü";

    bucketStore.createBucket(bucketName1);
    bucketStore.createBucket(bucketName2);
    bucketStore.createBucket(bucketName3);

    final List<BucketMetadata> buckets = bucketStore.listBuckets();

    assertThat(buckets.size()).as("FileStore should hold three Buckets").isEqualTo(3);
  }

  /**
   * Creates a bucket and checks that it can be retrieved by its name.
   *
   */
  @Test
  void shouldGetBucketByName() {
    bucketStore.createBucket(TEST_BUCKET_NAME);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucket).as("Bucket should not be null").isNotNull();
    assertThat(bucket.getName()).as("Bucket name should end with " + TEST_BUCKET_NAME)
        .isEqualTo(TEST_BUCKET_NAME);
  }

  /**
   * Checks if a bucket can be deleted.
   *
   */
  @Test
  void shouldDeleteBucket() {
    bucketStore.createBucket(TEST_BUCKET_NAME);
    boolean bucketDeleted = bucketStore.deleteBucket(TEST_BUCKET_NAME);
    BucketMetadata bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThat(bucketDeleted).as("Deletion should succeed!").isTrue();
    assertThat(bucket).as("Bucket should be null!").isNull();
  }

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  void cleanupStores() {
    for (final BucketMetadata bucket : bucketStore.listBuckets()) {
      bucketStore.deleteBucket(bucket.getName());
    }
  }
}
