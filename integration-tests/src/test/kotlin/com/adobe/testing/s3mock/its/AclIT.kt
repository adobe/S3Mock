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

import com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.AccessControlPolicy
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest
import software.amazon.awssdk.services.s3.model.Grant
import software.amazon.awssdk.services.s3.model.Grantee
import software.amazon.awssdk.services.s3.model.Owner
import software.amazon.awssdk.services.s3.model.Permission.FULL_CONTROL
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.Type.CANONICAL_USER
import java.io.File

internal class AclIT : S3TestBase() {

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "Owner and Grantee not available on test AWS account.")
  fun testGetAcl_noAcl(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    givenBucketAndObjectV2(testInfo, sourceKey)

    val acl = s3ClientV2.getObjectAcl(
      GetObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    )

    val owner = acl.owner()
    assertThat(owner.id()).isEqualTo(DEFAULT_OWNER.id)
    assertThat(owner.displayName()).isEqualTo(DEFAULT_OWNER.displayName)
    val grants = acl.grants()
    assertThat(grants).hasSize(1)
    val grant = grants[0]
    assertThat(grant.permission()).isEqualTo(FULL_CONTROL)
    val grantee = grant.grantee()
    assertThat(grantee).isNotNull
    assertThat(grantee.id()).isEqualTo(DEFAULT_OWNER.id)
    assertThat(grantee.displayName()).isEqualTo(DEFAULT_OWNER.displayName)
    assertThat(grantee.type()).isEqualTo(CANONICAL_USER)
  }


  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "Owner and Grantee not available on test AWS account.")
  fun testPutAndGetAcl(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val key = UPLOAD_FILE_NAME
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(
      CreateBucketRequest
        .builder()
        .bucket(bucketName)
        .objectLockEnabledForBucket(true)
        .build()
    )
    s3ClientV2.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(key).build(),
      RequestBody.fromFile(uploadFile)
    )

    val userId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ab"
    val userName = "John Doe"
    val granteeId = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
    val granteeName = "Jane Doe"
    val granteeEmail = "jane@doe.com"
    s3ClientV2.putObjectAcl(
      PutObjectAclRequest
        .builder()
        .bucket(bucketName)
        .key(key)
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
        .key(key)
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
