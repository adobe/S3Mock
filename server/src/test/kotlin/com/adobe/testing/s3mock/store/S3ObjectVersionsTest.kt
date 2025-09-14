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
package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.DtoTestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.util.UUID

internal class S3ObjectVersionsTest {
  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(
      S3ObjectVersions::class.java, testInfo
    )
    assertThat(iut).isNotNull()
    assertThat(iut.id).isEqualTo(UUID.fromString("c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c"))
    assertThat(iut.versions).hasSize(2)
    assertThat(iut.versions[0]).isEqualTo("796c301f-a714-4483-a0cc-9034f01c1a6d")
    assertThat(iut.versions[1]).isEqualTo("f51669d1-84f9-42d8-90ae-34c9f3bb3944")
    assertThat(iut.latestVersion).isEqualTo("f51669d1-84f9-42d8-90ae-34c9f3bb3944")
  }

  @Test
  fun testSerialization(testInfo: TestInfo) {

    val iut = S3ObjectVersions(
      UUID.fromString("c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c"),
      mutableListOf("796c301f-a714-4483-a0cc-9034f01c1a6d", "f51669d1-84f9-42d8-90ae-34c9f3bb3944")
    )
    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  @Test
  fun `latestVersion is null until first version is created`() {
    val iut = S3ObjectVersions(UUID.randomUUID())
    assertThat(iut.latestVersion).isNull()

    val version = iut.createVersion()
    assertThat(version).isNotBlank()
    assertThat(iut.latestVersion).isEqualTo(version)
  }

  @Test
  fun `creating multiple versions updates latest and deleting latest reverts to previous`() {
    val iut = S3ObjectVersions(UUID.randomUUID())

    val version1 = iut.createVersion()
    assertThat(version1).isNotBlank()
    val version2 = iut.createVersion()
    assertThat(version2).isNotBlank()
    assertThat(iut.latestVersion).isEqualTo(version2)

    iut.deleteVersion(version2)
    assertThat(iut.latestVersion).isEqualTo(version1)
  }
}
