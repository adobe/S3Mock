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

package com.adobe.testing.s3mock.its

import com.amazonaws.services.s3.AmazonS3
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

  private val s3Client: AmazonS3 = createS3ClientV1()

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "BucketOwner does not match owner in S3")
  fun testCreateBucketAndListAllBuckets(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    val bucket = s3Client.createBucket(bucketName)
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    val creationDate = Date(System.currentTimeMillis() / 1000 * 1000 - 60000)
    assertThat(bucket.name).isEqualTo(bucketName)

    val buckets = s3Client.listBuckets().stream()
      .filter { b: Bucket -> bucketName == b.name }
      .collect(Collectors.toList())
    assertThat(buckets).hasSize(1)

    val createdBucket = buckets[0]
    assertThat(createdBucket.creationDate).isAfterOrEqualTo(creationDate)

    createdBucket.owner.also {
      assertThat(it.displayName).isEqualTo("s3-mock-file-store")
      assertThat(it.id).isEqualTo("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "Default buckets do not exist in S3.")
  fun testDefaultBucketCreation() {
    val buckets = s3Client.listBuckets()
    val bucketNames = buckets.stream()
      .map { obj: Bucket -> obj.name }
      .filter { o: String? -> INITIAL_BUCKET_NAMES.contains(o) }
      .collect(Collectors.toSet())
    assertThat(bucketNames)
      .containsAll(INITIAL_BUCKET_NAMES)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCreateAndDeleteBucket(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)
    s3Client.headBucket(HeadBucketRequest(bucketName))
    s3Client.deleteBucket(bucketName)

    s3Client.doesBucketExistV2(bucketName).also {
      assertThat(it).isFalse
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
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
  @S3VerifiedSuccess(year = 2022)
  fun testBucketDoesExistV2_ok(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)

    s3Client.doesBucketExistV2(bucketName).also {
      assertThat(it).isTrue
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testBucketDoesExistV2_failure(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)

    val doesBucketExist = s3Client.doesBucketExistV2(bucketName)
    assertThat(doesBucketExist).isFalse
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun duplicateBucketCreation(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket(bucketName)

    assertThatThrownBy {
      s3Client.createBucket(bucketName)
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Service: Amazon S3; Status Code: 409; " +
        "Error Code: BucketAlreadyOwnedByYou;")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
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
