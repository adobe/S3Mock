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

import com.adobe.testing.s3mock.DtoTestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.net.URI

internal class AccessControlPolicyTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeXML(
      AccessControlPolicy::class.java, testInfo
    )

    val owner = iut.owner
    assertThat(owner).isNotNull()
    assertThat(owner!!.id).isEqualTo("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    assertThat(iut.accessControlList).hasSize(3)

    iut.accessControlList?.get(0).also {
      assertThat(it?.permission).isEqualTo(Grant.Permission.FULL_CONTROL)
      when (val grantee = it?.grantee) {
        is CanonicalUser -> {
                    assertThat(grantee.id).isEqualTo("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
          assertThat(grantee.displayName).isEqualTo("mtd@amazon.com")
        }
        else -> error("Expected CanonicalUser but was ${'$'}{grantee?.javaClass}")
      }
    }

    iut.accessControlList?.get(1).also {
      assertThat(it?.permission).isEqualTo(Grant.Permission.WRITE)
      when (val grantee = it?.grantee) {
                is Group -> assertThat(grantee.uri).isEqualTo(URI.create("http://acs.amazonaws.com/groups/s3/LogDelivery"))
        else -> error("Expected Group but was ${'$'}{grantee?.javaClass}")
      }
    }
    iut.accessControlList?.get(2).also {
      assertThat(it?.permission).isEqualTo(Grant.Permission.WRITE_ACP)
      when (val grantee = it?.grantee) {
                is AmazonCustomerByEmail -> assertThat(grantee.emailAddress).isEqualTo("xyz@amazon.com")
        else -> error("Expected AmazonCustomerByEmail but was ${'$'}{grantee?.javaClass}")
      }
    }
  }

  @Test
  fun testSerialization(testInfo: TestInfo) {
    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser("mtd@amazon.com", owner.id)
    val group = Group(URI.create("http://acs.amazonaws.com/groups/s3/LogDelivery"))
    val customer = AmazonCustomerByEmail("xyz@amazon.com")

    val iut = AccessControlPolicy(
      listOf(
        Grant(grantee, Grant.Permission.FULL_CONTROL),
        Grant(group, Grant.Permission.WRITE),
        Grant(customer, Grant.Permission.WRITE_ACP)
      ),
      owner
    )
    DtoTestUtil.serializeAndAssertXML(iut, testInfo)
  }
}
