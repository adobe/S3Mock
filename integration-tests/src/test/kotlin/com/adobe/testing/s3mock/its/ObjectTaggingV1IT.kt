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
class ObjectTaggingV1IT : S3TestBase() {

  @Test
  fun testPutAndGetObjectTagging(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val s3Object = s3Client!!.getObject(bucketName, UPLOAD_FILE_NAME)

    val tagList: MutableList<Tag> = ArrayList()
    val tag = Tag("foo", "bar")
    tagList.add(tag)
    val setObjectTaggingRequest =
      SetObjectTaggingRequest(bucketName, s3Object.key, ObjectTagging(tagList))
    s3Client!!.setObjectTagging(setObjectTaggingRequest)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)
    val getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.tagSet)
      .`as`("Couldn't find that the tag that was placed")
      .hasSize(1)
    assertThat(getObjectTaggingResult.tagSet).contains(tag)
  }

  @Test
  fun testPutObjectAndGetObjectTagging_withTagging(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val tagList: MutableList<Tag> = ArrayList()
    tagList.add(Tag("foo", "bar"))
    val putObjectRequest = PutObjectRequest(
      bucketName,
      uploadFile.name,
      uploadFile
    )
      .withTagging(ObjectTagging(tagList))
    s3Client!!.putObject(putObjectRequest)
    val s3Object = s3Client!!.getObject(bucketName, uploadFile.name)
    val getObjectTaggingRequest =
      GetObjectTaggingRequest(bucketName, s3Object.key)
    val getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.tagSet)
      .`as`("Couldn't find that the tag that was placed")
      .hasSize(1)
    assertThat(getObjectTaggingResult.tagSet[0].value)
      .`as`("The value of the tag placed did not match")
      .isEqualTo("bar")
  }

  /**
   * Verify that tagging with multiple tags can be obtained and returns expected content.
   */
  @Test
  fun testPutObjectAndGetObjectTagging_multipleTags(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val tagList: MutableList<Tag> = ArrayList()
    val tag1 = Tag("foo1", "bar1")
    val tag2 = Tag("foo2", "bar2")
    tagList.add(tag1)
    tagList.add(tag2)
    val putObjectRequest = PutObjectRequest(
      bucketName,
      uploadFile.name,
      uploadFile
    ).withTagging(ObjectTagging(tagList))
    s3Client!!.putObject(putObjectRequest)
    val s3Object = s3Client!!.getObject(bucketName, uploadFile.name)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)
    val getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    assertThat(getObjectTaggingResult.tagSet).hasSize(2)
    assertThat(getObjectTaggingResult.tagSet).contains(tag1, tag2)
  }


  @Test
  fun testGetObjectTagging_noTags(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val s3Object = s3Client!!.getObject(bucketName, UPLOAD_FILE_NAME)
    val getObjectTaggingRequest = GetObjectTaggingRequest(bucketName, s3Object.key)
    val getObjectTaggingResult = s3Client!!.getObjectTagging(getObjectTaggingRequest)

    // There shouldn't be any tags here
    assertThat(getObjectTaggingResult.tagSet).isEmpty()
  }
}
