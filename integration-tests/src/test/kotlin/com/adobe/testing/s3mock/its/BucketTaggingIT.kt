/*
 *  Copyright 2017-2026 Adobe.
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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Tag

internal class BucketTaggingIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedTodo
  fun `GET BucketTagging returns empty tag set when no tags are set`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val tagSet = s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet()

    assertThat(tagSet).isEmpty()
  }

  @Test
  @S3VerifiedTodo
  fun `PUT and GET BucketTagging succeeds`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("env", "prod")
    val tag2 = tag("team", "backend")

    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging { t -> t.tagSet(tag1, tag2) }
    }

    val tagSet = s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet()

    assertThat(tagSet).contains(tag1, tag2)
  }

  @Test
  @S3VerifiedTodo
  fun `PUT and GET and DELETE BucketTagging succeeds`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("env", "staging")
    val tag2 = tag("owner", "team-a")

    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging { t -> t.tagSet(tag1, tag2) }
    }

    assertThat(s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet())
      .contains(tag1, tag2)

    s3Client.deleteBucketTagging { it.bucket(bucketName) }

    assertThat(s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet())
      .isEmpty()
  }

  private fun tag(
    key: String,
    value: String,
  ): Tag =
    Tag
      .builder()
      .key(key)
      .value(value)
      .build()
}
