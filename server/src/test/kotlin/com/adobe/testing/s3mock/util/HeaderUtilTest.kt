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
package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

internal class HeaderUtilTest {
  @Test
  fun testGetUserMetadata_canonical() {
    val httpHeaders = HttpHeaders().apply { add(X_AMZ_CANONICAL_HEADER, TEST_VALUE) }

    val userMetadata = HeaderUtil.userMetadataFrom(httpHeaders)
    assertThat(userMetadata).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE)
  }

  @Test
  fun testGetUserMetadata_javaSdk() {
    val httpHeaders = HttpHeaders().apply { add(X_AMZ_LOWERCASE_HEADER, TEST_VALUE) }

    val userMetadata = HeaderUtil.userMetadataFrom(httpHeaders)
    assertThat(userMetadata).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE)
  }

  @Test
  fun testCreateUserMetadata_canonical() {
    val userMetadata = mapOf(X_AMZ_CANONICAL_HEADER to TEST_VALUE)
    val s3ObjectMetadata = s3ObjectMetadata(userMetadata = userMetadata)

    val userMetadataHeaders = s3ObjectMetadata.userMetadataHeaders()
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE)
  }

  @Test
  fun testCreateUserMetadata_javaSdk() {
    val userMetadata = mapOf(X_AMZ_LOWERCASE_HEADER to TEST_VALUE)
    val s3ObjectMetadata = s3ObjectMetadata(userMetadata = userMetadata)

    val userMetadataHeaders = s3ObjectMetadata.userMetadataHeaders()
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE)
  }

  private fun s3ObjectMetadata(
    id: UUID = UUID.randomUUID(),
    key: String = "key",
    userMetadata: Map<String, String>? = null
  ): S3ObjectMetadata =
    S3ObjectMetadata(
      id,
      key,
      "size",
      "lastModified",
      "\"etag\"",
      null,
      Instant.now().toEpochMilli(),
      Path.of("test"),
      userMetadata,
      null,
      null,
      null,
      Owner(0L.toString()),
      null,
      null,
      ChecksumAlgorithm.SHA256,
      null,
      StorageClass.STANDARD,
      null,
      null,
      false,
      ChecksumType.FULL_OBJECT
    )

  companion object {
    private const val X_AMZ_CANONICAL_HEADER = "X-Amz-Meta-Some-header"
    private const val X_AMZ_LOWERCASE_HEADER = "x-amz-meta-Some-header"
    private const val TEST_VALUE = "test-value"
  }
}
