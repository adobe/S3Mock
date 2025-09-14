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

import com.adobe.testing.s3mock.DtoTestUtil.serializeAndAssertXML
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class LocationConstraintTest {
  @Test
  fun testSerialization(testInfo: TestInfo) {
    val iut = LocationConstraint(Region.fromValue("us-west-2"))
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }

  @Test
  fun testSerialization_usEastOne(testInfo: TestInfo) {
    val iut = LocationConstraint(Region.fromValue("us-east-1"))
    assertThat(iut).isNotNull()
    serializeAndAssertXML(iut, testInfo)
  }
}
