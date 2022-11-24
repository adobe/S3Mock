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

package com.adobe.testing.s3mock.its

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.Bucket
import com.amazonaws.services.s3.model.HeadBucketRequest
import com.amazonaws.services.s3.model.PutObjectRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.File
import java.util.Date
import java.util.stream.Collectors

/**
 * Test the application using the AmazonS3 SDK V1.
 */
internal class BucketV1IT : S3TestBase() {

  @Test
  fun testCreateBucketAndListAllBuckets(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    val bucket = s3Client.createBucket(bucketName)
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    val creationDate = Date(System.currentTimeMillis() / 1000 * 1000 - 60000)
    assertThat(bucket.name)
      .`as`("Bucket name should match '$bucketName'!")
      .isEqualTo(bucketName)
    val buckets = s3Client.listBuckets().stream().filter { b: Bucket -> bucketName == b.name }
      .collect(Collectors.toList())
    assertThat(buckets).`as`("Expecting one bucket").hasSize(1)
    val createdBucket = buckets[0]
    assertThat(createdBucket.creationDate).isAfterOrEqualTo(creationDate)
    val bucketOwner = createdBucket.owner
    assertThat(bucketOwner.displayName).isEqualTo("s3-mock-file-store")
    assertThat(bucketOwner.id)
      .isEqualTo("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
  }

  @Test
  fun testDefaultBucketCreation() {
    val buckets = s3Client.listBuckets()
    val bucketNames = buckets.stream()
      .map { obj: Bucket -> obj.name }
      .filter { o: String? -> INITIAL_BUCKET_NAMES.contains(o) }
      .collect(Collectors.toSet())
    assertThat(bucketNames)
      .containsAll(INITIAL_BUCKET_NAMES)
      .`as`("Not all default Buckets got created")
  }

  @Test
  fun testCreateAndDeleteBucket(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)
    s3Client.headBucket(HeadBucketRequest(bucketName))
    s3Client.deleteBucket(bucketName)
    val doesBucketExist = s3Client.doesBucketExistV2(bucketName)
    assertThat(doesBucketExist)
      .`as`("Deleted Bucket should not exist!")
      .isFalse
  }

  @Test
  fun testFailureDeleteNonEmptyBucket(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client.putObject(PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile))
    assertThatThrownBy { s3Client.deleteBucket(bucketName) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 409; Error Code: BucketNotEmpty")
  }

  @Test
  fun testBucketDoesExistV2_ok(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)
    val doesBucketExist = s3Client.doesBucketExistV2(bucketName)
    assertThat(doesBucketExist)
      .`as`("The previously created bucket, '$bucketName', should exist!")
      .isTrue
  }

  @Test
  fun testBucketDoesExistV2_failure(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    val doesBucketExist = s3Client.doesBucketExistV2(bucketName)
    assertThat(doesBucketExist)
      .`as`("The bucket, '$bucketName', should not exist!")
      .isFalse
  }

  @Test
  fun duplicateBucketCreation(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)

    assertThatThrownBy {
      s3Client.createBucket(bucketName)
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Service: Amazon S3; Status Code: 409; " +
        "Error Code: BucketAlreadyExists;")
  }

  @Test
  fun duplicateBucketDeletion(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)

    s3Client.deleteBucket(bucketName)

    assertThatThrownBy {
      s3Client.deleteBucket(bucketName)
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket;")
  }
}
