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
import com.amazonaws.services.s3.model.GetObjectTaggingRequest
import com.amazonaws.services.s3.model.ObjectTagging
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SetObjectTaggingRequest
import com.amazonaws.services.s3.model.Tag
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.File

/**
 * Test the application using the AmazonS3 SDK V1.
 */
internal class ObjectTaggingV1IT : S3TestBase() {
  val s3Client: AmazonS3 = createS3ClientV1()

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutAndGetObjectTagging(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val s3Object = s3Client.getObject(bucketName, UPLOAD_FILE_NAME)

    val tag = Tag("foo", "bar")
    val tagList: MutableList<Tag> = mutableListOf(tag)
    val setObjectTaggingRequest = SetObjectTaggingRequest(bucketName, s3Object.key, ObjectTagging(tagList))
    s3Client.setObjectTagging(setObjectTaggingRequest)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)

    s3Client.getObjectTagging(getObjectTaggingRequest).also {
      // There should be 'foo:bar' here
      assertThat(it.tagSet).hasSize(1)
      assertThat(it.tagSet).contains(tag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutObjectAndGetObjectTagging_withTagging(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val putObjectRequest = PutObjectRequest(
      bucketName,
      uploadFile.name,
      uploadFile
    )
      .withTagging(ObjectTagging(mutableListOf(Tag("foo", "bar"))))
    s3Client.putObject(putObjectRequest)
    val s3Object = s3Client.getObject(bucketName, uploadFile.name)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)

    s3Client.getObjectTagging(getObjectTaggingRequest).also {
      // There should be 'foo:bar' here
      assertThat(it.tagSet).hasSize(1)
      assertThat(it.tagSet[0].value).isEqualTo("bar")
    }
  }

  /**
   * Verify that tagging with multiple tags can be obtained and returns expected content.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutObjectAndGetObjectTagging_multipleTags(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val tag1 = Tag("foo1", "bar1")
    val tag2 = Tag("foo2", "bar2")
    val tagList: MutableList<Tag> = mutableListOf(tag1, tag2)
    val putObjectRequest = PutObjectRequest(
      bucketName,
      uploadFile.name,
      uploadFile
    ).withTagging(ObjectTagging(tagList))
    s3Client.putObject(putObjectRequest)
    val s3Object = s3Client.getObject(bucketName, uploadFile.name)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)

    s3Client.getObjectTagging(getObjectTaggingRequest).also {
      assertThat(it.tagSet).hasSize(2)
      assertThat(it.tagSet).contains(tag1, tag2)
    }
  }


  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetObjectTagging_noTags(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val s3Object = s3Client.getObject(bucketName, UPLOAD_FILE_NAME)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)

    s3Client.getObjectTagging(getObjectTaggingRequest).also {
      // There shouldn't be any tags here
      assertThat(it.tagSet).isEmpty()
    }
  }
}
