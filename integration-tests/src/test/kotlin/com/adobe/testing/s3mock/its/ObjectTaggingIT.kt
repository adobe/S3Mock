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
  fun testGetObjectTagging_noTags(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putObject({
        it.bucket(bucketName)
        it.key("foo")
      },
      RequestBody.fromString("foo")
    )

    assertThat(s3Client.getObjectTagging {
      it.bucket(bucketName)
      it.key("foo")
    }.tagSet()).isEmpty()
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutAndGetObjectTagging(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)
    val tag1 = Tag.builder().key("tag1").value("foo").build()
    val tag2 = Tag.builder().key("tag2").value("bar").build()

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
  fun testPutObjectAndGetObjectTagging_withTagging(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    s3Client.putObject({
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
    ).contains(Tag.builder().key("msv").value("foo").build())
  }

  /**
   * Verify that tagging with multiple tags can be obtained and returns expected content.
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObjectAndGetObjectTagging_multipleTags(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = Tag.builder().key("tag1").value("foo").build()
    val tag2 = Tag.builder().key("tag2").value("bar").build()

    s3Client.putObject({
        it.bucket(bucketName)
          it.key("multipleFoo")
          it.tagging(Tagging.builder().tagSet(tag1, tag2).build())
      }, RequestBody.fromString("multipleFoo")
    )

    assertThat(
      s3Client.getObjectTagging {
        it.bucket(bucketName)
        it.key("multipleFoo")
      }.tagSet()
    ).contains(
      tag1,
      tag2
    )
  }
}
