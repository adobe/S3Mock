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

import com.adobe.testing.s3mock.dto.DtoTestUtil.deserialize
import com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.IOException

internal class ObjectLockConfigurationTest {
  @Test
  @Throws(IOException::class)
  fun testSerialization(testInfo: TestInfo) {
    val retention = DefaultRetention(1, null, Mode.COMPLIANCE)
    val rule = ObjectLockRule(retention)
    val iut = ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule)
    assertThat(iut).isNotNull()
    serializeAndAssert(iut, testInfo)
  }


  @Test
  @Throws(IOException::class)
  fun testDeserialization(testInfo: TestInfo) {
    val iut = deserialize(ObjectLockConfiguration::class.java, testInfo)
    assertThat(iut.objectLockEnabled()).isNull()
    assertThat(iut.objectLockRule().defaultRetention().years()).isEqualTo(1)
    assertThat(iut.objectLockRule().defaultRetention().mode()).isEqualTo(Mode.COMPLIANCE)
  }
}
