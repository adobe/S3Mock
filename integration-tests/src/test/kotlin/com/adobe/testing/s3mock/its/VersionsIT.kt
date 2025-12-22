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
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.ObjectVersionStorageClass
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.StorageClass

internal class VersionsIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testListObjectVersions_nonVersionEnabledBucket(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    givenObject(bucketName, UPLOAD_FILE_NAME)
    val listObjectVersions = s3Client.listObjectVersions { it.bucket(bucketName) }
    assertThat(listObjectVersions.hasVersions()).isTrue
    assertThat(listObjectVersions.versions()[0].key()).isEqualTo(UPLOAD_FILE_NAME)
    assertThat(listObjectVersions.versions()[0].versionId()).isEqualTo("null")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testListObjectVersions_versionEnabledBucket(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId = givenObject(bucketName, UPLOAD_FILE_NAME).versionId()

    val listObjectVersions = s3Client.listObjectVersions { it.bucket(bucketName) }
    assertThat(listObjectVersions.hasVersions()).isTrue
    assertThat(listObjectVersions.versions()[0].key()).isEqualTo(UPLOAD_FILE_NAME)
    assertThat(listObjectVersions.versions()[0].versionId()).isEqualTo(versionId)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetObject_withVersion(testInfo: TestInfo) {
    val expectedChecksum = DigestUtil.checksumFor(UPLOAD_FILE_PATH, DefaultChecksumAlgorithm.SHA1)
    val bucketName = givenBucket(testInfo)

    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    s3Client
      .getObjectAttributes {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.versionId(versionId)
        it.objectAttributes(
          ObjectAttributes.OBJECT_SIZE,
          ObjectAttributes.STORAGE_CLASS,
          ObjectAttributes.E_TAG,
          ObjectAttributes.CHECKSUM,
        )
      }.also {
        assertThat(it.versionId()).isEqualTo(versionId)
        // GetObjectAttributes returns the default storageClass "STANDARD", even though other APIs may not.
        assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
        assertThat(it.objectSize()).isEqualTo(UPLOAD_FILE_LENGTH)
        assertThat(it.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetObject_withMultipleVersions(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId1 =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    val versionId2 =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.versionId(versionId2)
      }.use {
        assertThat(it.response().versionId()).isEqualTo(versionId2)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.versionId(versionId1)
      }.use {
        assertThat(it.response().versionId()).isEqualTo(versionId1)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.use {
        assertThat(it.response().versionId()).isEqualTo(versionId2)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetDeleteObject_withVersion(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    val versionId1 =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    val versionId2 =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.versionId(versionId2)
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.use {
        assertThat(it.response().versionId()).isEqualTo(versionId1)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetDeleteObject_withDeleteMarker(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).versionId()

    val versionId =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).versionId()

    s3Client
      .deleteObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.also {
        assertThat(it.deleteMarker()).isEqualTo(true)
      }

    val listObjectVersions = s3Client.listObjectVersions { it.bucket(bucketName) }
    assertThat(listObjectVersions.hasVersions()).isTrue
    assertThat(listObjectVersions.versions()[0].key()).isEqualTo(UPLOAD_FILE_NAME)
    assertThat(listObjectVersions.versions()[0].versionId()).isEqualTo(versionId)
    // ListObjectVersions returns the default storageClass "STANDARD", even though other APIs may not.
    assertThat(listObjectVersions.versions()[0].storageClass()).isEqualTo(ObjectVersionStorageClass.STANDARD)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }
}
