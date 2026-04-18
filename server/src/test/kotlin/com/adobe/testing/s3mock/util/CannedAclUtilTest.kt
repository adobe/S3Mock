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

import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.Group
import com.adobe.testing.s3mock.dto.ObjectCannedACL
import com.adobe.testing.s3mock.dto.Owner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class CannedAclUtilTest {
  @Test
  fun `private ACL grants only full control to owner`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PRIVATE)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    assertThat(policy.accessControlList).hasSize(1)
    assertThat(policy.accessControlList!![0].permission).isEqualTo(Grant.Permission.FULL_CONTROL)
  }

  @Test
  fun `public-read ACL grants full control to owner and read to AllUsers group`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PUBLIC_READ)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    assertThat(policy.accessControlList).hasSize(2)
    val permissions = policy.accessControlList!!.map { it.permission }
    assertThat(permissions).contains(Grant.Permission.FULL_CONTROL, Grant.Permission.READ)
    val allUsersGrant = policy.accessControlList!!.first { it.grantee is Group }
    assertThat((allUsersGrant.grantee as Group).uri).isEqualTo(Group.ALL_USERS_URI)
    assertThat(allUsersGrant.permission).isEqualTo(Grant.Permission.READ)
  }

  @Test
  fun `public-read-write ACL grants read and write to AllUsers group`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.PUBLIC_READ_WRITE)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    // owner full control + AllUsers READ + AllUsers WRITE
    assertThat(policy.accessControlList).hasSize(3)
    val groupGrants = policy.accessControlList!!.filter { it.grantee is Group }
    assertThat(groupGrants).hasSize(2)
    val groupPermissions = groupGrants.map { it.permission }
    assertThat(groupPermissions).containsExactlyInAnyOrder(Grant.Permission.READ, Grant.Permission.WRITE)
    groupGrants.forEach { grant ->
      assertThat((grant.grantee as Group).uri).isEqualTo(Group.ALL_USERS_URI)
    }
  }

  @Test
  fun `aws-exec-read ACL grants only full control to owner`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.AWS_EXEC_READ)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    assertThat(policy.accessControlList).hasSize(1)
    assertThat(policy.accessControlList!![0].permission).isEqualTo(Grant.Permission.FULL_CONTROL)
  }

  @Test
  fun `authenticated-read ACL grants read to AuthenticatedUsers group`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.AUTHENTICATED_READ)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    assertThat(policy.accessControlList).hasSize(2)
    val groupGrant = policy.accessControlList!!.first { it.grantee is Group }
    assertThat((groupGrant.grantee as Group).uri).isEqualTo(Group.AUTHENTICATED_USERS_URI)
    assertThat(groupGrant.permission).isEqualTo(Grant.Permission.READ)
  }

  @Test
  fun `bucket-owner-read ACL grants read to bucket owner`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.BUCKET_OWNER_READ)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    assertThat(policy.accessControlList).hasSize(2)
    val ownerGrant = policy.accessControlList!!.first { it.permission == Grant.Permission.FULL_CONTROL }
    assertThat(ownerGrant).isNotNull()
    val bucketOwnerGrant = policy.accessControlList!!.first { it.permission == Grant.Permission.READ }
    assertThat(bucketOwnerGrant.grantee).isNotNull()
  }

  @Test
  fun `bucket-owner-full-control ACL grants full control to both owner and bucket owner`() {
    val policy = CannedAclUtil.policyForCannedAcl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)

    assertThat(policy.owner).isEqualTo(Owner.DEFAULT_OWNER)
    // owner FULL_CONTROL + bucket-owner READ
    assertThat(policy.accessControlList).hasSize(2)
    val permissions = policy.accessControlList!!.map { it.permission }
    assertThat(permissions).contains(Grant.Permission.FULL_CONTROL, Grant.Permission.READ)
  }
}
