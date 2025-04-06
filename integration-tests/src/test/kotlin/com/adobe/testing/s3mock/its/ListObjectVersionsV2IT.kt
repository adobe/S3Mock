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
import org.assertj.core.groups.Tuple
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus
import java.io.File

internal class ListObjectVersionsV2IT : S3TestBase() {
  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun listObjectVersions(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val version1 = s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-1")
      },
      RequestBody.fromFile(uploadFile)
    ).versionId()

    val version2 = s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-2")
      },
      RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.listObjectVersions {
      it.bucket(bucketName)
    }.also {
      assertThat(it.versions())
        .hasSize(2)
        .extracting("versionId", "isLatest")
        .containsExactlyInAnyOrder(Tuple(version1, true), Tuple(version2, true))
    }
  }

  @Test
  @S3VerifiedTodo
  fun listObjectVersions_noVersioning(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-1")
      },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-2")
      },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.listObjectVersions {
      it.bucket(bucketName)
    }.also {
      assertThat(it.versions())
        .hasSize(2)
        .extracting("versionId")
        .containsExactlyInAnyOrder("null", "null")
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun listObjectVersions_deleteMarker(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val version1 = s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-1")
      },
      RequestBody.fromFile(uploadFile)
    ).versionId()

    val version2 = s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-2")
      },
      RequestBody.fromFile(uploadFile)
    ).versionId()

    val version3 = s3ClientV2.putObject(
      {
        it.bucket(bucketName)
        it.key("$UPLOAD_FILE_NAME-3")
      },
      RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.deleteObject {
      it.bucket(bucketName)
      it.key("$UPLOAD_FILE_NAME-3")
    }

    s3ClientV2.listObjectVersions {
      it.bucket(bucketName)
    }.also {
      assertThat(it.versions())
        .hasSize(3)
        .extracting("versionId", "isLatest")
        .containsExactlyInAnyOrder(Tuple(version1, true), Tuple(version2, true), Tuple(version3, false))
      assertThat(it.deleteMarkers())
        .hasSize(1)
        .extracting("key")
        .containsExactlyInAnyOrder("$UPLOAD_FILE_NAME-3")
    }
  }
}
