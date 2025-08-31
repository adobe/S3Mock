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
package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

internal class ObjectTaggingIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET ObjectTagging succeeds with no tags`(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromString("foo")
    )

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).isEmpty()
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT and GET ObjectTagging succeeds`(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)
    val tag1 = tag("tag1" to "foo")
    val tag2 = tag("tag2" to "bar")

    s3Client.putObjectTagging {
      it.bucket(bucketName)
      it.key(key)
      it.tagging {
        it.tagSet(tag1, tag2)
      }
    }

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).contains(
      tag1,
      tag2
    )
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT and GET and DELETE ObjectTagging succeeds`(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)
    val tag1 = tag("tag1" to "foo")
    val tag2 = tag("tag2" to "bar")

    s3Client.putObjectTagging {
      it.bucket(bucketName)
      it.key(key)
      it.tagging {
        it.tagSet(tag1, tag2)
      }
    }

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).contains(
      tag1,
      tag2
    )

    s3Client.deleteObjectTagging {
      it.bucket(bucketName)
      it.key(key)
    }

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).isEmpty()
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT Object with tag string literal and GET ObjectTagging succeeds`(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
        it.tagging("msv=foo")
      },
      RequestBody.fromString("foo")
    )

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).contains(tag("msv" to "foo"))
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT Object with tag object and GET ObjectTagging succeeds`(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("tag1" to "foo")
    val tag2 = tag("tag2" to "bar")

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
        it.tagging(Tagging.builder().tagSet(tag1, tag2).build())
      },
      RequestBody.fromString("foo")
    )

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key(key)
      }.tagSet()
    ).contains(
      tag1,
      tag2
    )
  }

  private fun tag(key: String, value: String): Tag = Tag.builder().key(key).value(value).build()

  private fun tag(pair: Pair<String, String>): Tag = tag(pair.first, pair.second)
}
