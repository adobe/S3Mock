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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.ObjectVersion
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File

internal class ListObjectVersionsV2IT : S3TestBase() {
  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPutObjects_listObjectVersions(testInfo: TestInfo) {
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

    s3ClientV2.listObjectVersions(
      ListObjectVersionsRequest.builder()
        .bucket(bucketName)
        .build()
    ).also {
      assertThat(it.versions())
        .hasSize(2)
        .extracting(ObjectVersion::checksumAlgorithm)
        .containsOnly(
          Tuple(arrayListOf(ChecksumAlgorithm.SHA256)),
          Tuple(arrayListOf(ChecksumAlgorithm.SHA256))
        )
    }
  }
}
