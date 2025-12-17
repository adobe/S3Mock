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

package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.Group
import com.adobe.testing.s3mock.dto.ObjectCannedACL
import com.adobe.testing.s3mock.dto.Owner

/**
 * Utility class with helper methods to get canned ACLs.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html)
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
 */
object CannedAclUtil {
  @JvmStatic
  fun policyForCannedAcl(cannedAcl: ObjectCannedACL): AccessControlPolicy =
    when (cannedAcl) {
      ObjectCannedACL.PRIVATE -> privateAcl()
      ObjectCannedACL.PUBLIC_READ -> publicReadAcl()
      ObjectCannedACL.PUBLIC_READ_WRITE -> publicReadWriteAcl()
      ObjectCannedACL.AWS_EXEC_READ -> awsExecReadAcl()
      ObjectCannedACL.AUTHENTICATED_READ -> authenticatedReadAcl()
      ObjectCannedACL.BUCKET_OWNER_READ -> bucketOwnerReadAcl()
      ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL -> bucketOwnerFullControlAcl()
    }

  private val defaultOwner = Owner.DEFAULT_OWNER
  private val defaultOwnerUser = CanonicalUser("s3-mock-file-store", defaultOwner.id)

  private fun policyWithOwner(vararg additionalGrants: Grant): AccessControlPolicy =
    AccessControlPolicy(
      listOf(Grant(defaultOwnerUser, Grant.Permission.FULL_CONTROL)) + additionalGrants,
      defaultOwner
    )

  private fun bucketOwnerFullControlAcl(): AccessControlPolicy =
    policyWithOwner(
      Grant(
        CanonicalUser(
          "s3-mock-file-store",
          Owner.DEFAULT_OWNER_BUCKET.id
        ),
        Grant.Permission.READ
      )
    )

  private fun bucketOwnerReadAcl(): AccessControlPolicy =
    policyWithOwner(
      Grant(
        CanonicalUser(
          "s3-mock-file-store",
          Owner.DEFAULT_OWNER_BUCKET.id
        ),
        Grant.Permission.READ
      )
    )

  private fun authenticatedReadAcl(): AccessControlPolicy =
    policyWithOwner(
      Grant(
        Group(Group.AUTHENTICATED_USERS_URI),
        Grant.Permission.READ
      )
    )

  /**
   * The documentation says that EC2 gets READ access. Not sure what to configure for that.
   */
  private fun awsExecReadAcl(): AccessControlPolicy = policyWithOwner()

  private fun publicReadWriteAcl(): AccessControlPolicy =
    policyWithOwner(
      Grant(
        Group(Group.ALL_USERS_URI),
        Grant.Permission.READ
      ),
      Grant(
        Group(Group.ALL_USERS_URI),
        Grant.Permission.WRITE
      )
    )

  private fun publicReadAcl(): AccessControlPolicy =
    policyWithOwner(
      Grant(
        Group(Group.ALL_USERS_URI),
        Grant.Permission.READ
      )
    )

  private fun privateAcl(): AccessControlPolicy = policyWithOwner()
}
