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
package com.adobe.testing.s3mock.s3.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

internal class UuidUtilTest {
  @Test
  fun `uuidV7 creates a version 7 UUID with the timestamp in its first 48 bits`() {
    val timestamp = Instant.parse("2026-11-14T12:34:56.789Z")

    val uuid = UuidUtil.uuidV7(timestamp)

    assertThat(uuid.version()).isEqualTo(7)
    assertThat(uuid.variant()).isEqualTo(2)
    assertThat(uuid.mostSignificantBits ushr 16).isEqualTo(timestamp.toEpochMilli())
  }

  @Test
  fun `uuidV7 strings sort by timestamp`() {
    val start = Instant.parse("2026-11-14T12:34:56.789Z")
    val timestamps =
      listOf(
        start,
        start.plusMillis(1),
        start.plusSeconds(1),
        start.plus(Duration.ofDays(365)),
      )

    val uuids = timestamps.map { UuidUtil.uuidV7(it).toString() }

    assertThat(uuids.shuffled().sorted()).containsExactlyElementsOf(uuids)
  }

  @Test
  fun `uuidV7 creates distinct UUIDs for the same timestamp`() {
    val timestamp = Instant.now()

    val uuids = (1..1000).map { UuidUtil.uuidV7(timestamp) }

    assertThat(uuids).doesNotHaveDuplicates()
  }
}
