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

package com.adobe.testing.s3mock.service;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.store.BucketStore;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BucketService {

  private final BucketStore bucketStore;

  public BucketService(BucketStore bucketStore) {
    this.bucketStore = bucketStore;
  }

  public boolean isBucketEmpty(String name) {
    return bucketStore.isBucketEmpty(name);
  }

  public boolean doesBucketExist(String name) {
    return bucketStore.doesBucketExist(name);
  }

  public List<Bucket> listBuckets() {
    return bucketStore
        .listBuckets()
        .stream()
        .filter(Objects::nonNull)
        .map(Bucket::from)
        .collect(Collectors.toList());
  }

  /**
   * Retrieves a Bucket identified by its name.
   *
   * @param name of the Bucket to be retrieved
   *
   * @return the Bucket or null if not found
   */
  public Bucket getBucket(String name) {
    return Bucket.from(bucketStore.getBucketMetadata(name));
  }

  /**
   * Creates a Bucket identified by its name.
   *
   * @param name of the Bucket to be created
   *
   * @return the Bucket
   */
  public Bucket createBucket(String name) {
    return Bucket.from(bucketStore.createBucket(name));
  }

  public boolean deleteBucket(String name) {
    return bucketStore.deleteBucket(name);
  }

  public void verifyBucketExists(String bucketName) {
    if (!bucketStore.doesBucketExist(bucketName)) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchBucket",
          "The specified bucket does not exist.");
    }
  }

  public void verifyBucketExists(Bucket bucket) {
    if (bucket == null) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchBucket",
          "The specified bucket does not exist.");
    }
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">API Reference Bucket Naming</a>.
   */
  public void verifyBucketNameIsAllowed(String bucketName) {
    if (!bucketName.matches("[a-z0-9.-]+")) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidBucketName",
          "The specified bucket is not valid.");
    }
  }

  public void verifyBucketIsEmpty(String bucketName) {
    if (!bucketStore.isBucketEmpty(bucketName)) {
      throw new S3Exception(CONFLICT.value(), "BucketNotEmpty",
          "The bucket you tried to delete is not empty.");
    }
  }

  public void verifyBucketDoesNotExist(String bucketName) {
    if (bucketStore.doesBucketExist(bucketName)) {
      throw new S3Exception(CONFLICT.value(), "BucketAlreadyExists",
          "The requested bucket name is not available. "
              + "The bucket namespace is shared by all users of the system. "
              + "Please select a different name and try again.");
    }
  }
}
