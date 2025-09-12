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

internal class BucketLifecycleConfigurationTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserialize(
      BucketLifecycleConfiguration::class.java, testInfo
    )

    val rules = iut.rules!!
    assertThat(rules).hasSize(2)

    rules[0].also {
      assertThat(it.id).isEqualTo("id1")
      assertThat(it.expiration).isNull()
      it.filter.also {
        assertThat(it).isNotNull()
        assertThat(it?.prefix).isEqualTo("documents/")
      }
      assertThat(it.status).isEqualTo(LifecycleRule.Status.ENABLED)
      it.transitions.also {
        assertThat(it!!).hasSize(1)
        assertThat(it[0].date).isNull()
        assertThat(it[0].days).isEqualTo(30)
        assertThat(it[0].storageClass).isEqualTo(StorageClass.GLACIER)
      }
      assertThat(it.abortIncompleteMultipartUpload).isNull()
      assertThat(it.noncurrentVersionExpiration).isNull()
      assertThat(it.noncurrentVersionTransitions).isNull()
    }

    rules[1].also {
      assertThat(it.id).isEqualTo("id2")
      it.filter.also {
        assertThat(it!!).isNotNull()
        assertThat(it.prefix).isEqualTo("logs/")
      }
      assertThat(it.status).isEqualTo(LifecycleRule.Status.ENABLED)
      assertThat(it.expiration).isNotNull()
      assertThat(it.expiration?.days).isEqualTo(365)
      assertThat(it.abortIncompleteMultipartUpload).isNull()
      assertThat(it.noncurrentVersionExpiration).isNull()
      assertThat(it.noncurrentVersionTransitions).isNull()
    }
  }

  @Test
  fun testSerialization(testInfo: TestInfo) {
    val filter1 = LifecycleRuleFilter(null, null, "documents/", null, null)
    val transition1 = Transition(null, 30, StorageClass.GLACIER)
    val rule1 = LifecycleRule(
      null, null, filter1, "id1", null, null,
      LifecycleRule.Status.ENABLED, listOf(transition1)
    )
    val filter2 = LifecycleRuleFilter(null, null, "logs/", null, null)
    val expiration2 = LifecycleExpiration(null, 365, null)
    val rule2 = LifecycleRule(
      null, expiration2, filter2, "id2", null, null,
      LifecycleRule.Status.ENABLED, null
    )
    val iut = BucketLifecycleConfiguration(listOf(rule1, rule2))
    assertThat(iut).isNotNull()
    DtoTestUtil.serializeAndAssert(iut, testInfo)
  }
}
