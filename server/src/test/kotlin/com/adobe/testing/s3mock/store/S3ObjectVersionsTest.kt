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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

internal class S3ObjectVersionsTest {
  @Test
  fun testVersion_isNull() {
    val iut = S3ObjectVersions(UUID.randomUUID())
    assertThat(iut.latestVersion).isNull()

    val version = iut.createVersion()
    assertThat(version).isNotBlank()
    assertThat(iut.latestVersion).isEqualTo(version)
  }

  @Test
  fun testVersion_createVersion() {
    val iut = S3ObjectVersions(UUID.randomUUID())

    val version = iut.createVersion()
    assertThat(version).isNotBlank()
    assertThat(iut.latestVersion).isEqualTo(version)
  }

  @Test
  fun testVersion_createVersionsDeleteVersion() {
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
