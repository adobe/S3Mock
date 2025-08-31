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
package com.adobe.testing.s3mock.dto

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException
import java.net.URI

internal class AccessControlPolicyTest {
  @Test
  @Throws(IOException::class)
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserialize(
      AccessControlPolicy::class.java, testInfo
    )

    val owner = iut.owner
    assertThat(owner).isNotNull()
    assertThat(owner.id).isEqualTo(
      "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    assertThat(owner.displayName).isEqualTo("mtd@amazon.com")
    assertThat(iut.accessControlList).hasSize(3)

    iut.accessControlList[0].also {
      assertThat(it.permission).isEqualTo(Grant.Permission.FULL_CONTROL)
      assertThat(it.grantee).isNotNull()
      assertThat(it.grantee).isInstanceOf(CanonicalUser::class.java)
      val user = it.grantee as CanonicalUser
      assertThat(user.id()).isEqualTo("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
      assertThat(user.displayName()).isEqualTo("mtd@amazon.com")
    }

    iut.accessControlList[1].also {
      assertThat(it.permission).isEqualTo(Grant.Permission.WRITE)
      assertThat(it.grantee).isNotNull()
      assertThat(it.grantee).isInstanceOf(Group::class.java)
      val group = it.grantee as Group
      assertThat(group.uri()).isEqualTo(URI.create("http://acs.amazonaws.com/groups/s3/LogDelivery"))
    }
    iut.accessControlList[2].also {
      assertThat(it.permission).isEqualTo(Grant.Permission.WRITE_ACP)
      assertThat(it.grantee).isNotNull()
      assertThat(it.grantee).isInstanceOf(AmazonCustomerByEmail::class.java)
      val customer = it.grantee as AmazonCustomerByEmail
      assertThat(customer.emailAddress()).isEqualTo("xyz@amazon.com")
    }
  }

  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val owner = Owner(
        "mtd@amazon.com",
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    val grantee = CanonicalUser(owner.displayName, owner.id)
    val group = Group(URI.create("http://acs.amazonaws.com/groups/s3/LogDelivery"))
    val customer = AmazonCustomerByEmail("xyz@amazon.com")

    val iut = AccessControlPolicy(
      owner,
      listOf(
        Grant(grantee, Grant.Permission.FULL_CONTROL),
        Grant(group, Grant.Permission.WRITE),
        Grant(customer, Grant.Permission.WRITE_ACP)
      )
    )
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
