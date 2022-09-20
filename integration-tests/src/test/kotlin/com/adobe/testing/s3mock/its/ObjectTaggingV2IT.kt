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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

class ObjectTaggingV2IT : S3TestBase() {

  @Test
  fun testGetObjectTagging_noTags(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2!!.putObject(
      { b: PutObjectRequest.Builder -> b.bucket(bucketName).key("foo") },
      RequestBody.fromString("foo")
    )

    assertThat(s3ClientV2!!.getObjectTagging { b: GetObjectTaggingRequest.Builder ->
      b.bucket(
        bucketName
      ).key("foo")
    }
      .tagSet())
      .isEmpty()
  }

  @Test
  fun testPutAndGetObjectTagging(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = "foo"
    val tag1 = Tag.builder().key("tag1").value("foo").build()
    val tag2 = Tag.builder().key("tag2").value("bar").build()
    s3ClientV2!!.putObject(
      { b: PutObjectRequest.Builder -> b.bucket(bucketName).key(key) },
      RequestBody.fromString("foo")
    )

    s3ClientV2!!.putObjectTagging(
      PutObjectTaggingRequest.builder().bucket(bucketName).key(key)
        .tagging(Tagging.builder().tagSet(tag1, tag2).build()).build()
    )

    assertThat(s3ClientV2!!.getObjectTagging { b: GetObjectTaggingRequest.Builder ->
      b.bucket(
        bucketName
      ).key(key)
    }
      .tagSet())
      .contains(
        tag1,
        tag2
      )
  }

  @Test
  fun testPutObjectAndGetObjectTagging_withTagging(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2!!.putObject(
      { b: PutObjectRequest.Builder -> b.bucket(bucketName).key("foo").tagging("msv=foo") },
      RequestBody.fromString("foo")
    )

    assertThat(s3ClientV2!!.getObjectTagging { b: GetObjectTaggingRequest.Builder ->
      b.bucket(
        bucketName
      ).key("foo")
    }
      .tagSet())
      .contains(Tag.builder().key("msv").value("foo").build())
  }

  /**
   * Verify that tagging with multiple tags can be obtained and returns expected content.
   */
  @Test
  fun testPutObjectAndGetObjectTagging_multipleTags(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val tag1 = Tag.builder().key("tag1").value("foo").build()
    val tag2 = Tag.builder().key("tag2").value("bar").build()

    s3ClientV2!!.putObject(
      { b: PutObjectRequest.Builder ->
        b.bucket(bucketName).key("multipleFoo")
          .tagging(Tagging.builder().tagSet(tag1, tag2).build())
      }, RequestBody.fromString("multipleFoo")
    )

    assertThat(s3ClientV2!!.getObjectTagging { b: GetObjectTaggingRequest.Builder ->
      b.bucket(
        bucketName
      ).key("multipleFoo")
    }
      .tagSet())
      .contains(
        tag1,
        tag2
      )
  }
}
