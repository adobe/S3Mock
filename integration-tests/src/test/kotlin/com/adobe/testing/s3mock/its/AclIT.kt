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

import com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AccessControlPolicy
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest
import software.amazon.awssdk.services.s3.model.Grant
import software.amazon.awssdk.services.s3.model.Grantee
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.Owner
import software.amazon.awssdk.services.s3.model.Permission.FULL_CONTROL
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest
import software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER

internal class AclIT : S3TestBase() {
  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  fun testPutCannedAcl_OK(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)

    s3ClientV2.putObjectAcl(
      PutObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .acl(ObjectCannedACL.PRIVATE)
        .build()
    ).also {
      assertThat(it.sdkHttpResponse().isSuccessful).isTrue()
    }

    s3ClientV2.getObjectAcl(
      GetObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    ).also {
      assertThat(it.sdkHttpResponse().isSuccessful).isTrue()
      assertThat(it.owner().id()).isEqualTo(DEFAULT_OWNER.id)
      assertThat(it.owner().displayName()).isEqualTo(DEFAULT_OWNER.displayName)
      assertThat(it.grants().size).isEqualTo(1)
      assertThat(it.grants()[0].permission()).isEqualTo(FULL_CONTROL)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "Owner and Grantee not available on test AWS account.")
  fun testGetAcl_noAcl(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)

    val acl = s3ClientV2.getObjectAcl(
      GetObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    )

    acl.owner().also { owner ->
      assertThat(owner.id()).isEqualTo(DEFAULT_OWNER.id)
      assertThat(owner.displayName()).isEqualTo(DEFAULT_OWNER.displayName)
    }

    val grants = acl.grants().also {
      assertThat(it).hasSize(1)
    }
    val grant = grants[0]
    assertThat(grant.permission()).isEqualTo(FULL_CONTROL)
    grant.grantee().also {
      assertThat(it).isNotNull
      assertThat(it.id()).isEqualTo(DEFAULT_OWNER.id)
      assertThat(it.displayName()).isEqualTo(DEFAULT_OWNER.displayName)
      assertThat(it.type()).isEqualTo(CANONICAL_USER)
    }
  }


  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "Owner and Grantee not available on test AWS account.")
  fun testPutAndGetAcl(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)

    val userId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ab"
    val userName = "John Doe"
    val granteeId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
    val granteeName = "Jane Doe"
    val granteeEmail = "jane@doe.com"
    s3ClientV2.putObjectAcl(
      PutObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .accessControlPolicy(
          AccessControlPolicy
            .builder()
            .owner(Owner.builder().id(userId).displayName(userName).build())
            .grants(
              Grant.builder()
                .permission(FULL_CONTROL)
                .grantee(
                  Grantee.builder().id(granteeId).displayName(granteeName)
                    .type(CANONICAL_USER).build()
                ).build()
            ).build()
        )
        .build()
    )

    val acl = s3ClientV2.getObjectAcl(
      GetObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    )
    val owner = acl.owner()
    assertThat(owner).isNotNull
    assertThat(owner.id()).isEqualTo(userId)
    assertThat(owner.displayName()).isEqualTo(userName)

    assertThat(acl.grants()).hasSize(1)

    val grant = acl.grants()[0]
    assertThat(grant.permission()).isEqualTo(FULL_CONTROL)

    val grantee = grant.grantee()
    assertThat(grantee).isNotNull
    assertThat(grantee.id()).isEqualTo(granteeId)
    assertThat(grantee.displayName()).isEqualTo(granteeName)
    assertThat(grantee.type()).isEqualTo(CANONICAL_USER)
  }
}
