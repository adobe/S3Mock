/*
 *  Copyright 2017-2023 Adobe.
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
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.File

internal class ListObjectV2IT : S3TestBase() {

  @Test
  @S3VerifiedTodo
  fun testPutObjectsListObjectsV2_checksumAlgorithm_sha256(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key("$UPLOAD_FILE_NAME-1")
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key("$UPLOAD_FILE_NAME-2")
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val listObjectsV2Response = s3ClientV2.listObjectsV2(
      ListObjectsV2Request.builder()
        .bucket(bucketName)
        .build()
    )

    assertThat(listObjectsV2Response.contents())
      .hasSize(2)
      .extracting(S3Object::checksumAlgorithm)
      .containsOnly(
        Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
        Tuple(arrayListOf(ChecksumAlgorithm.SHA256))
      )
  }

  @Test
  @S3VerifiedTodo
  fun testPutObjectsListObjectsV1_checksumAlgorithm_sha256(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key("$UPLOAD_FILE_NAME-1")
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key("$UPLOAD_FILE_NAME-2")
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val listObjectsResponse = s3ClientV2.listObjects(
      ListObjectsRequest.builder()
        .bucket(bucketName)
        .build()
    )

    assertThat(listObjectsResponse.contents())
      .hasSize(2)
      .extracting(S3Object::checksumAlgorithm)
      .containsOnly(
        Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
        Tuple(arrayListOf(ChecksumAlgorithm.SHA256))
      )
  }

  /**
   * Test list with safe characters in keys.
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedTodo
  fun shouldListV2WithCorrectObjectNames(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val weirdStuff = ("!-_.*'()") //safe characters as per S3 API
    val prefix = "shouldListWithCorrectObjectNames/"
    val key = prefix + weirdStuff + uploadFile.name + weirdStuff
    s3ClientV2.putObject(PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .build(),
      RequestBody.fromFile(uploadFile)
    )
    val listing = s3ClientV2.listObjectsV2(
      ListObjectsV2Request
        .builder()
        .bucket(bucketName)
        .prefix(prefix)
        .encodingType(EncodingType.URL)
        .build()
    )
    val summaries = listing.contents()
    assertThat(summaries)
      .`as`("Must have exactly one match")
      .hasSize(1)
    assertThat(summaries[0].key())
      .`as`("Object name must match")
      .isEqualTo(key)
  }

}
