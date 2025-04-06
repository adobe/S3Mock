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

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.StorageClass
import java.io.File

internal class VersionsV2IT : S3TestBase() {
  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedTodo
  fun testPutGetObject_withVersion(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), Algorithm.SHA1)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId = s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.getObjectAttributes {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.versionId(versionId)
      it.objectAttributes(
        ObjectAttributes.OBJECT_SIZE,
        ObjectAttributes.STORAGE_CLASS,
        ObjectAttributes.E_TAG,
        ObjectAttributes.CHECKSUM
      )
    }.also {
      //
      assertThat(it.versionId()).isEqualTo(versionId)
      //default storageClass is STANDARD, which is never returned from APIs
      assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
      assertThat(it.objectSize()).isEqualTo(File(UPLOAD_FILE_NAME).length())
      assertThat(it.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetObject_withMultipleVersions(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId1 = s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    val versionId2 = s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.versionId(versionId2)
    }.also {
      assertThat(it.response().versionId()).isEqualTo(versionId2)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.versionId(versionId1)
    }.also {
      assertThat(it.response().versionId()).isEqualTo(versionId1)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.also {
      assertThat(it.response().versionId()).isEqualTo(versionId2)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetDeleteObject_withVersion(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId1 = s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    val versionId2 = s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.versionId(versionId2)
    }.also {
      assertThat(it.deleteMarker()).isEqualTo(false)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.also {
      assertThat(it.response().versionId()).isEqualTo(versionId1)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetDeleteObject_withDeleteMarker(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.putObject(
      {
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
      }, RequestBody.fromFile(uploadFile)
    ).versionId()

    s3ClientV2.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.also {
      assertThat(it.deleteMarker()).isEqualTo(true)
    }

    assertThatThrownBy {
      s3ClientV2.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }
}
