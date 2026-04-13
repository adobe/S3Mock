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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.dto.ObjectCannedACL
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource

internal class ObjectCannedAclHeaderConverterTest {
  private val iut = ObjectCannedAclHeaderConverter()

  @ParameterizedTest
  @CsvSource(
    "private,PRIVATE",
    "public-read,PUBLIC_READ",
    "public-read-write,PUBLIC_READ_WRITE",
    "authenticated-read,AUTHENTICATED_READ",
    "aws-exec-read,AWS_EXEC_READ",
    "bucket-owner-read,BUCKET_OWNER_READ",
    "bucket-owner-full-control,BUCKET_OWNER_FULL_CONTROL",
  )
  fun `should convert supported canned acl values`(value: String, expected: ObjectCannedACL) {
    assertThat(iut.convert(value)).isEqualTo(expected)
  }

  @ParameterizedTest
  @ValueSource(strings = ["", "public_read", "PUBLIC-READ", "unknown"])
  fun `should return null for unsupported canned acl values`(value: String) {
    assertThat(iut.convert(value)).isNull()
  }
}
