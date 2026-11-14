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

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * Utility class for creating time-ordered UUIDs.
 * [RFC 9562](https://www.rfc-editor.org/rfc/rfc9562#name-uuid-version-7)
 */
object UuidUtil {
  /**
   * Creates a version 7 UUID for [timestamp]. String forms sort by timestamp, to the millisecond.
   * UUIDs created in the same millisecond sort randomly.
   */
  fun uuidV7(timestamp: Instant): UUID {
    val random = ThreadLocalRandom.current()
    val mostSigBits = (timestamp.toEpochMilli() shl 16) or (0x7L shl 12) or (random.nextLong() and 0xFFFL)
    val leastSigBits = (random.nextLong() and 0x3FFF_FFFF_FFFF_FFFFL) or (0b10L shl 62)
    return UUID(mostSigBits, leastSigBits)
  }
}
