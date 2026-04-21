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
package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders.AWS_CHUNKED
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32C
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC64NVME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA1
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_TYPE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
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

  // --- storeHeadersFrom ---

  @Test
  fun `storeHeadersFrom extracts content-disposition`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=test.txt") }
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=test.txt")
  }

  @Test
  fun `storeHeadersFrom extracts cache-control`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CACHE_CONTROL, "no-cache") }
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.CACHE_CONTROL, "no-cache")
  }

  @Test
  fun `storeHeadersFrom extracts expires`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.EXPIRES, "Thu, 01 Dec 2025 16:00:00 GMT") }
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.EXPIRES, "Thu, 01 Dec 2025 16:00:00 GMT")
  }

  @Test
  fun `storeHeadersFrom extracts content-language`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CONTENT_LANGUAGE, "en-US") }
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_LANGUAGE, "en-US")
  }

  @Test
  fun `storeHeadersFrom excludes aws-chunked only content-encoding`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED) }
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).doesNotContainKey(HttpHeaders.CONTENT_ENCODING)
  }

  @Test
  fun `storeHeadersFrom includes content-encoding when combined with other encodings`() {
    val headers =
      HttpHeaders().apply {
        add(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED)
        add(HttpHeaders.CONTENT_ENCODING, "gzip")
      }
    // Content-Encoding is retained because it contains more than just aws-chunked
    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsKey(HttpHeaders.CONTENT_ENCODING)
  }

  // --- encryptionHeadersFrom ---

  @Test
  fun `encryptionHeadersFrom extracts server-side encryption header`() {
    val headers = HttpHeaders().apply { add(X_AMZ_SERVER_SIDE_ENCRYPTION, "aws:kms") }
    val result = HeaderUtil.encryptionHeadersFrom(headers)
    assertThat(result).containsEntry(X_AMZ_SERVER_SIDE_ENCRYPTION, "aws:kms")
  }

  @Test
  fun `encryptionHeadersFrom returns empty map when no encryption header`() {
    val headers = HttpHeaders()
    val result = HeaderUtil.encryptionHeadersFrom(headers)
    assertThat(result).isEmpty()
  }

  // --- isV4Signed ---

  @Test
  fun `isV4Signed returns true for STREAMING-AWS4-HMAC-SHA256-PAYLOAD`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CONTENT_SHA256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") }
    assertThat(HeaderUtil.isV4Signed(headers)).isTrue()
  }

  @Test
  fun `isV4Signed returns true for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CONTENT_SHA256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER") }
    assertThat(HeaderUtil.isV4Signed(headers)).isTrue()
  }

  @Test
  fun `isV4Signed returns false when header absent`() {
    assertThat(HeaderUtil.isV4Signed(HttpHeaders())).isFalse()
  }

  @Test
  fun `isV4Signed returns false for other sha256 value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CONTENT_SHA256, "some-hash-value") }
    assertThat(HeaderUtil.isV4Signed(headers)).isFalse()
  }

  // --- isChunkedEncoding ---

  @Test
  fun `isChunkedEncoding returns true when aws-chunked is present`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED) }
    assertThat(HeaderUtil.isChunkedEncoding(headers)).isTrue()
  }

  @Test
  fun `isChunkedEncoding returns false when content-encoding absent`() {
    assertThat(HeaderUtil.isChunkedEncoding(HttpHeaders())).isFalse()
  }

  @Test
  fun `isChunkedEncoding returns false for non-chunked encoding`() {
    val headers = HttpHeaders().apply { add(HttpHeaders.CONTENT_ENCODING, "gzip") }
    assertThat(HeaderUtil.isChunkedEncoding(headers)).isFalse()
  }

  // --- mediaTypeFrom ---

  @Test
  fun `mediaTypeFrom parses valid media type`() {
    val mediaType = HeaderUtil.mediaTypeFrom("application/json")
    assertThat(mediaType).isEqualTo(MediaType.APPLICATION_JSON)
  }

  @Test
  fun `mediaTypeFrom returns octet-stream for null content type`() {
    val mediaType = HeaderUtil.mediaTypeFrom(null)
    assertThat(mediaType).isEqualTo(MediaType.APPLICATION_OCTET_STREAM)
  }

  @Test
  fun `mediaTypeFrom returns octet-stream for invalid content type`() {
    val mediaType = HeaderUtil.mediaTypeFrom("not/a/valid/media/type")
    assertThat(mediaType).isEqualTo(MediaType.APPLICATION_OCTET_STREAM)
  }

  // --- overrideHeadersFrom ---

  @Test
  fun `overrideHeadersFrom maps response-content-type to Content-Type`() {
    val params = mapOf("response-content-type" to "text/plain")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_TYPE, "text/plain")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-disposition`() {
    val params = mapOf("response-content-disposition" to "attachment")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_DISPOSITION, "attachment")
  }

  @Test
  fun `overrideHeadersFrom maps response-cache-control`() {
    val params = mapOf("response-cache-control" to "max-age=3600")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.CACHE_CONTROL, "max-age=3600")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-encoding`() {
    val params = mapOf("response-content-encoding" to "gzip")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_ENCODING, "gzip")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-language`() {
    val params = mapOf("response-content-language" to "fr")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_LANGUAGE, "fr")
  }

  @Test
  fun `overrideHeadersFrom maps response-expires`() {
    val params = mapOf("response-expires" to "Sat, 01 Jan 2028 00:00:00 GMT")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).containsEntry(HttpHeaders.EXPIRES, "Sat, 01 Jan 2028 00:00:00 GMT")
  }

  @Test
  fun `overrideHeadersFrom ignores unknown query params`() {
    val params = mapOf("unknown-param" to "value")
    val result = HeaderUtil.overrideHeadersFrom(params)
    assertThat(result).isEmpty()
  }

  // --- checksumHeaderFrom ---

  @Test
  fun `checksumHeaderFrom returns header for SHA256`() {
    val result = HeaderUtil.checksumHeaderFrom("abc123", ChecksumAlgorithm.SHA256)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_SHA256, "abc123")
  }

  @Test
  fun `checksumHeaderFrom returns empty map when checksum is null`() {
    val result = HeaderUtil.checksumHeaderFrom(null, ChecksumAlgorithm.SHA256)
    assertThat(result).isEmpty()
  }

  @Test
  fun `checksumHeaderFrom returns empty map when algorithm is null`() {
    val result = HeaderUtil.checksumHeaderFrom("abc123", null)
    assertThat(result).isEmpty()
  }

  // --- checksumAlgorithmFromHeader ---

  @Test
  fun `checksumAlgorithmFromHeader detects SHA256`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_SHA256, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.SHA256)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects SHA1`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_SHA1, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.SHA1)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects CRC32`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC32, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC32)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects CRC32C`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC32C, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC32C)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects CRC64NVME`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC64NVME, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC64NVME)
  }

  @Test
  fun `checksumAlgorithmFromHeader returns null when no checksum header present`() {
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(HttpHeaders())).isNull()
  }

  // --- checksumAlgorithmFromSdk ---

  @Test
  fun `checksumAlgorithmFromSdk detects SHA256 from sdk header`() {
    val headers = HttpHeaders().apply { add(X_AMZ_SDK_CHECKSUM_ALGORITHM, "SHA256") }
    assertThat(HeaderUtil.checksumAlgorithmFromSdk(headers)).isEqualTo(ChecksumAlgorithm.SHA256)
  }

  @Test
  fun `checksumAlgorithmFromSdk returns null when header absent`() {
    assertThat(HeaderUtil.checksumAlgorithmFromSdk(HttpHeaders())).isNull()
  }

  // --- checksumTypeFrom ---

  @Test
  fun `checksumTypeFrom detects FULL_OBJECT from header`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_TYPE, "FULL_OBJECT") }
    assertThat(HeaderUtil.checksumTypeFrom(headers)).isEqualTo(ChecksumType.FULL_OBJECT)
  }

  @Test
  fun `checksumTypeFrom returns null when header absent`() {
    assertThat(HeaderUtil.checksumTypeFrom(HttpHeaders())).isNull()
  }

  // --- checksumFrom ---

  @Test
  fun `checksumFrom extracts SHA256 checksum value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_SHA256, "sha256value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("sha256value")
  }

  @Test
  fun `checksumFrom extracts SHA1 checksum value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_SHA1, "sha1value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("sha1value")
  }

  @Test
  fun `checksumFrom extracts CRC32 checksum value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC32, "crc32value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc32value")
  }

  @Test
  fun `checksumFrom extracts CRC32C checksum value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC32C, "crc32cvalue") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc32cvalue")
  }

  @Test
  fun `checksumFrom extracts CRC64NVME checksum value`() {
    val headers = HttpHeaders().apply { add(X_AMZ_CHECKSUM_CRC64NVME, "crc64value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc64value")
  }

  @Test
  fun `checksumFrom returns null when no checksum header present`() {
    assertThat(HeaderUtil.checksumFrom(HttpHeaders())).isNull()
  }

  // --- S3ObjectMetadata extension functions ---

  @Test
  fun `versionHeader returns version id header when versioning enabled`() {
    val metadata = s3ObjectMetadata(versionId = "v1")
    val result = metadata.versionHeader(versioning = true)
    assertThat(result).containsEntry(AwsHttpHeaders.X_AMZ_VERSION_ID, "v1")
  }

  @Test
  fun `versionHeader returns empty map when versioning disabled`() {
    val metadata = s3ObjectMetadata(versionId = "v1")
    val result = metadata.versionHeader(versioning = false)
    assertThat(result).isEmpty()
  }

  @Test
  fun `versionHeader returns empty map when version id is null`() {
    val metadata = s3ObjectMetadata(versionId = null)
    val result = metadata.versionHeader(versioning = true)
    assertThat(result).isEmpty()
  }

  @Test
  fun `checksumHeader returns checksum header for SHA256`() {
    val metadata = s3ObjectMetadata(checksum = "abc", checksumAlgorithm = ChecksumAlgorithm.SHA256)
    val result = metadata.checksumHeader()
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_SHA256, "abc")
  }

  @Test
  fun `checksumHeader returns empty map when checksum is null`() {
    val metadata = s3ObjectMetadata(checksum = null, checksumAlgorithm = null)
    val result = metadata.checksumHeader()
    assertThat(result).isEmpty()
  }

  @Test
  fun `storageClassHeaders returns empty map for STANDARD storage class`() {
    val metadata = s3ObjectMetadata(storageClass = StorageClass.STANDARD)
    val result = metadata.storageClassHeaders()
    assertThat(result).isEmpty()
  }

  @Test
  fun `storageClassHeaders returns header for non-standard storage class`() {
    val metadata = s3ObjectMetadata(storageClass = StorageClass.GLACIER)
    val result = metadata.storageClassHeaders()
    assertThat(result).containsEntry(AwsHttpHeaders.X_AMZ_STORAGE_CLASS, "GLACIER")
  }

  @Test
  fun `storageClassHeaders returns empty map when storage class is null`() {
    val metadata = s3ObjectMetadata(storageClass = null)
    val result = metadata.storageClassHeaders()
    assertThat(result).isEmpty()
  }

  private fun s3ObjectMetadata(
    id: UUID = UUID.randomUUID(),
    key: String = "key",
    userMetadata: Map<String, String>? = null,
    versionId: String? = null,
    checksum: String? = null,
    checksumAlgorithm: ChecksumAlgorithm? = ChecksumAlgorithm.SHA256,
    storageClass: StorageClass? = StorageClass.STANDARD,
  ): S3ObjectMetadata =
    S3ObjectMetadata(
      id,
      key,
      "size",
      "lastModified",
      "\"etag\"",
      null, // contentType
      Instant.now().toEpochMilli(),
      Path.of("test"),
      userMetadata,
      null,
      null,
      null,
      Owner(0L.toString()),
      null,
      null,
      checksumAlgorithm,
      checksum,
      storageClass,
      null,
      versionId,
      false,
      ChecksumType.FULL_OBJECT,
    )

  companion object {
    private const val X_AMZ_CANONICAL_HEADER = "X-Amz-Meta-Some-header"
    private const val X_AMZ_LOWERCASE_HEADER = "x-amz-meta-Some-header"
    private const val TEST_VALUE = "test-value"
  }
}
