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

import com.adobe.testing.s3mock.dto.Owner.Companion.DEFAULT_OWNER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Grant
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.ObjectOwnership
import software.amazon.awssdk.services.s3.model.Permission.FULL_CONTROL
import software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER

internal class AclIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `put canned ACL returns OK, get ACL returns the ACL`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)

    // create bucket that sets ownership to non-default to allow setting ACLs.
    s3Client
      .createBucket {
        it.bucket(bucketName)
        it.objectOwnership(ObjectOwnership.OBJECT_WRITER)
      }.also {
        assertThat(it.sdkHttpResponse().isSuccessful).isTrue()
      }

    givenObject(bucketName, sourceKey)

    s3Client
      .putObjectAcl {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.acl(ObjectCannedACL.PRIVATE)
      }.also {
        assertThat(it.sdkHttpResponse().isSuccessful).isTrue()
      }

    s3Client
      .getObjectAcl {
        it.bucket(bucketName)
        it.key(sourceKey)
      }.also { resp ->
        assertThat(resp.sdkHttpResponse().isSuccessful).isTrue()
        assertThat(resp.owner().id()).isNotBlank()
        assertThat(resp.grants()).hasSize(1)
        assertThat(resp.grants().first().permission()).isEqualTo(FULL_CONTROL)
      }
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "Owner and Grantee not available on test AWS account.",
  )
  fun `get ACL returns canned 'private' ACL`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)

    val acl =
      s3Client.getObjectAcl {
        it.bucket(bucketName)
        it.key(sourceKey)
      }

    acl.owner().also { owner ->
      assertThat(owner.id()).isEqualTo(DEFAULT_OWNER.id)
    }

    acl.grants().also {
      assertThat(it).hasSize(1)
    }

    acl
      .grants()
      .first()
      .also { grant ->
        assertThat(grant.permission()).isEqualTo(FULL_CONTROL)
      }.grantee()
      .also { grantee ->
        assertThat(grantee).isNotNull
        assertThat(grantee.id()).isEqualTo(DEFAULT_OWNER.id)
        assertThat(grantee.type()).isEqualTo(CANONICAL_USER)
      }
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "Owner and Grantee not available on test AWS account.",
  )
  fun `put ACL returns OK, get ACL returns the ACL`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)

    val userId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ab"
    val userName = "John Doe"
    val granteeId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
    val granteeName = "Jane Doe"
    s3Client.putObjectAcl {
      it.bucket(bucketName)
      it.key(sourceKey)
      it.accessControlPolicy {
        it.owner {
          it.id(userId)
        }
        it
          .grants(
            Grant
              .builder()
              .permission(FULL_CONTROL)
              .grantee {
                it.id(granteeId)
                it.displayName(granteeName)
                it.type(CANONICAL_USER)
              }.build(),
          ).build()
      }
    }

    val acl =
      s3Client.getObjectAcl {
        it.bucket(bucketName)
        it.key(sourceKey)
      }
    acl.owner().also {
      assertThat(it).isNotNull
      assertThat(it.id()).isEqualTo(userId)
    }

    assertThat(acl.grants()).hasSize(1)

    acl
      .grants()[0]
      .also {
        assertThat(it.permission()).isEqualTo(FULL_CONTROL)
      }.grantee()
      .also {
        assertThat(it).isNotNull
        assertThat(it.id()).isEqualTo(granteeId)
        assertThat(it.displayName()).isEqualTo(granteeName)
        assertThat(it.type()).isEqualTo(CANONICAL_USER)
      }
  }
}
