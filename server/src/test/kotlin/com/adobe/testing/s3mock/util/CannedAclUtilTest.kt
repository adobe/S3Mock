/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.Group
import com.adobe.testing.s3mock.dto.ObjectCannedACL
import com.adobe.testing.s3mock.dto.Owner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class CannedAclUtilTest {
  @Test
  fun `should create owner-only acl for private and aws-exec-read`() {
    val privatePolicy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PRIVATE)
    val awsExecReadPolicy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.AWS_EXEC_READ)

    assertThat(privatePolicy).isEqualTo(ownerOnlyPolicy())
    assertThat(awsExecReadPolicy).isEqualTo(ownerOnlyPolicy())
  }

  @Test
  fun `should create read grants for public and authenticated acl variants`() {
    val publicReadPolicy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PUBLIC_READ)
    val authenticatedReadPolicy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.AUTHENTICATED_READ)

    assertThat(publicReadPolicy)
      .isEqualTo(
        policyWithAdditionalGrants(
          Grant(Group(Group.ALL_USERS_URI), Grant.Permission.READ),
        ),
      )
    assertThat(authenticatedReadPolicy)
      .isEqualTo(
        policyWithAdditionalGrants(
          Grant(Group(Group.AUTHENTICATED_USERS_URI), Grant.Permission.READ),
        ),
      )
  }

  @Test
  fun `should create public read write acl with read and write grants`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PUBLIC_READ_WRITE)

    assertThat(policy)
      .isEqualTo(
        policyWithAdditionalGrants(
          Grant(Group(Group.ALL_USERS_URI), Grant.Permission.READ),
          Grant(Group(Group.ALL_USERS_URI), Grant.Permission.WRITE),
        ),
      )
  }

  @Test
  fun `should create bucket owner acl variants with default owner bucket read grant`() {
    val bucketOwnerRead = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.BUCKET_OWNER_READ)
    val bucketOwnerFullControl = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)

    val expected =
      policyWithAdditionalGrants(
        Grant(
          CanonicalUser("s3-mock-file-store", Owner.DEFAULT_OWNER_BUCKET.id),
          Grant.Permission.READ,
        ),
      )

    assertThat(bucketOwnerRead).isEqualTo(expected)
    assertThat(bucketOwnerFullControl).isEqualTo(expected)
  }

  private fun ownerOnlyPolicy(): AccessControlPolicy = policyWithAdditionalGrants()

  private fun policyWithAdditionalGrants(vararg grants: Grant): AccessControlPolicy =
    AccessControlPolicy(
      listOf(
        Grant(
          CanonicalUser("s3-mock-file-store", Owner.DEFAULT_OWNER.id),
          Grant.Permission.FULL_CONTROL,
        ),
      ) + grants,
      Owner.DEFAULT_OWNER,
    )
}
