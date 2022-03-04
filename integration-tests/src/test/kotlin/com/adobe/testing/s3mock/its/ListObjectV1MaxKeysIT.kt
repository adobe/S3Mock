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

import com.amazonaws.services.s3.model.ListObjectsRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ListObjectV1MaxKeysIT : S3TestBase() {
  private val bucketName = "some-bucket"

  @BeforeEach
  override fun prepareS3Client() {
    super.prepareS3Client()
    s3Client!!.createBucket(bucketName)
    s3Client!!.putObject(bucketName, "a", "")
    s3Client!!.putObject(bucketName, "b", "")
  }

  @Test
  fun returnsLimitedAmountOfObjectsBasedOnMaxKeys() {
    val request = ListObjectsRequest().withBucketName(bucketName).withMaxKeys(1)
    val objectListing = s3Client!!.listObjects(request)
    assertThat(objectListing.objectSummaries).hasSize(1)
    assertThat(objectListing.maxKeys).isEqualTo(1)
  }

  @Test
  fun returnsAllObjectsIfMaxKeysIsDefault() {
    val request = ListObjectsRequest().withBucketName(bucketName)
    val objectListing = s3Client!!.listObjects(request)
    assertThat(objectListing.objectSummaries).hasSize(2)
    assertThat(objectListing.maxKeys).isEqualTo(1000)
  }

  @Test
  fun returnsAllObjectsIfMaxKeysEqualToAmountOfObjects() {
    val request = ListObjectsRequest().withBucketName(bucketName)
      .withMaxKeys(2)
    val objectListing = s3Client!!.listObjects(request)
    assertThat(objectListing.objectSummaries).hasSize(2)
    assertThat(objectListing.maxKeys).isEqualTo(2)
  }

  @Test
  fun returnsAllObjectsIfMaxKeysMoreThanAmountOfObjects() {
    val request = ListObjectsRequest().withBucketName(bucketName)
      .withMaxKeys(3)
    val objectListing = s3Client!!.listObjects(request)
    assertThat(objectListing.objectSummaries).hasSize(2)
    assertThat(objectListing.maxKeys).isEqualTo(3)
  }

  @Test
  fun returnsEmptyListIfMaxKeysIsZero() {
    val request = ListObjectsRequest().withBucketName(bucketName)
      .withMaxKeys(0)
    val objectListing = s3Client!!.listObjects(request)
    assertThat(objectListing.objectSummaries).hasSize(0)
    assertThat(objectListing.maxKeys).isEqualTo(0)
  }

  @Test
  fun returnsAllObjectsIfMaxKeysIsNegative() {
    val request = ListObjectsRequest().withBucketName(bucketName)
      .withMaxKeys(-1)
    val objectListing = s3Client!!.listObjects(request)

    // Apparently, the Amazon SDK rejects negative max keys, and by default it's 1000
    assertThat(objectListing.objectSummaries).hasSize(2)
    assertThat(objectListing.maxKeys).isEqualTo(1000)
  }
}
