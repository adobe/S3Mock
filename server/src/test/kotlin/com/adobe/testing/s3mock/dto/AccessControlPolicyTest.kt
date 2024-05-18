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
package com.adobe.testing.s3mock.dto

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException

internal class AccessControlPolicyTest {
  @Test
  @Throws(IOException::class)
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserialize(
      AccessControlPolicy::class.java, testInfo
    )

    val owner = iut.getOwner()
    assertThat(owner).isNotNull()
    assertThat(owner.id).isEqualTo(
      "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    assertThat(owner.displayName).isEqualTo("mtd@amazon.com")
    assertThat(iut.getAccessControlList()).hasSize(1)

    iut.getAccessControlList()[0].also {
      assertThat(it.permission).isEqualTo(Grant.Permission.FULL_CONTROL)
      assertThat(it.grantee).isNotNull()
      assertThat(it.grantee.id()).isEqualTo("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
      assertThat(it.grantee.displayName()).isEqualTo("mtd@amazon.com")
      assertThat(it.grantee).isInstanceOf(CanonicalUser::class.java)
    }
  }

  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val owner = Owner(
      "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
      "mtd@amazon.com"
    )
    val grantee = CanonicalUser(owner.id, owner.displayName, null, null)
    val iut = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
