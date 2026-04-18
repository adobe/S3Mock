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
import com.adobe.testing.s3mock.dto.CopySource
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
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_VERSION_ID
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_VERSION_ID
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

  @Test
  fun `storeHeadersFrom captures content-disposition`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"test.txt\"") }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsKey(HttpHeaders.CONTENT_DISPOSITION)
  }

  @Test
  fun `storeHeadersFrom captures cache-control`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CACHE_CONTROL, "no-cache") }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.CACHE_CONTROL, "no-cache")
  }

  @Test
  fun `storeHeadersFrom captures content-language`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CONTENT_LANGUAGE, "en-US") }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsEntry(HttpHeaders.CONTENT_LANGUAGE, "en-US")
  }

  @Test
  fun `storeHeadersFrom captures content-encoding when not only aws-chunked`() {
    val headers =
      HttpHeaders().apply {
        add(HttpHeaders.CONTENT_ENCODING, "gzip")
        add(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED)
      }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsKey(HttpHeaders.CONTENT_ENCODING)
  }

  @Test
  fun `storeHeadersFrom skips content-encoding when only aws-chunked`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED) }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).doesNotContainKey(HttpHeaders.CONTENT_ENCODING)
  }

  @Test
  fun `storeHeadersFrom captures expires`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.EXPIRES, "Thu, 01 Jan 2099 00:00:00 GMT") }

    val result = HeaderUtil.storeHeadersFrom(headers)
    assertThat(result).containsKey(HttpHeaders.EXPIRES)
  }

  @Test
  fun `encryptionHeadersFrom captures server-side-encryption headers`() {
    val headers =
      HttpHeaders().apply {
        set(X_AMZ_SERVER_SIDE_ENCRYPTION, "aws:kms")
        set("$X_AMZ_SERVER_SIDE_ENCRYPTION-aws-kms-key-id", "arn:aws:kms:us-east-1:123456789012:key/some-key-id")
      }

    val result = HeaderUtil.encryptionHeadersFrom(headers)
    assertThat(result).containsKey(X_AMZ_SERVER_SIDE_ENCRYPTION)
    assertThat(result).containsKey("$X_AMZ_SERVER_SIDE_ENCRYPTION-aws-kms-key-id")
  }

  @Test
  fun `isV4Signed returns true for STREAMING-AWS4-HMAC-SHA256-PAYLOAD`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CONTENT_SHA256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") }

    assertThat(HeaderUtil.isV4Signed(headers)).isTrue()
  }

  @Test
  fun `isV4Signed returns true for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CONTENT_SHA256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER") }

    assertThat(HeaderUtil.isV4Signed(headers)).isTrue()
  }

  @Test
  fun `isV4Signed returns false when header is absent`() {
    assertThat(HeaderUtil.isV4Signed(HttpHeaders())).isFalse()
  }

  @Test
  fun `isV4Signed returns false for other sha256 values`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CONTENT_SHA256, "UNSIGNED-PAYLOAD") }

    assertThat(HeaderUtil.isV4Signed(headers)).isFalse()
  }

  @Test
  fun `isChunkedEncoding returns true when aws-chunked present`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CONTENT_ENCODING, AWS_CHUNKED) }

    assertThat(HeaderUtil.isChunkedEncoding(headers)).isTrue()
  }

  @Test
  fun `isChunkedEncoding returns false when aws-chunked absent`() {
    val headers = HttpHeaders().apply { set(HttpHeaders.CONTENT_ENCODING, "gzip") }

    assertThat(HeaderUtil.isChunkedEncoding(headers)).isFalse()
  }

  @Test
  fun `isChunkedEncoding returns false when no content-encoding header`() {
    assertThat(HeaderUtil.isChunkedEncoding(HttpHeaders())).isFalse()
  }

  @Test
  fun `mediaTypeFrom returns octet-stream for null input`() {
    assertThat(HeaderUtil.mediaTypeFrom(null)).isEqualTo(MediaType.APPLICATION_OCTET_STREAM)
  }

  @Test
  fun `mediaTypeFrom returns octet-stream for invalid media type`() {
    assertThat(HeaderUtil.mediaTypeFrom("not/a/valid/media/type///")).isEqualTo(MediaType.APPLICATION_OCTET_STREAM)
  }

  @Test
  fun `mediaTypeFrom returns parsed media type for valid input`() {
    assertThat(HeaderUtil.mediaTypeFrom("application/json")).isEqualTo(MediaType.APPLICATION_JSON)
  }

  @Test
  fun `overrideHeadersFrom maps response-content-type`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-content-type" to "text/plain"))
    assertThat(result).containsEntry(HttpHeaders.CONTENT_TYPE, "text/plain")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-language`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-content-language" to "en"))
    assertThat(result).containsEntry(HttpHeaders.CONTENT_LANGUAGE, "en")
  }

  @Test
  fun `overrideHeadersFrom maps response-expires`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-expires" to "Thu, 01 Jan 2099 00:00:00 GMT"))
    assertThat(result).containsEntry(HttpHeaders.EXPIRES, "Thu, 01 Jan 2099 00:00:00 GMT")
  }

  @Test
  fun `overrideHeadersFrom maps response-cache-control`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-cache-control" to "no-cache"))
    assertThat(result).containsEntry(HttpHeaders.CACHE_CONTROL, "no-cache")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-disposition`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-content-disposition" to "attachment"))
    assertThat(result).containsEntry(HttpHeaders.CONTENT_DISPOSITION, "attachment")
  }

  @Test
  fun `overrideHeadersFrom maps response-content-encoding`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("response-content-encoding" to "gzip"))
    assertThat(result).containsEntry(HttpHeaders.CONTENT_ENCODING, "gzip")
  }

  @Test
  fun `overrideHeadersFrom ignores unknown query params`() {
    val result = HeaderUtil.overrideHeadersFrom(mapOf("unknown-param" to "value"))
    assertThat(result).isEmpty()
  }

  @Test
  fun `checksumHeaderFrom returns empty map when both null`() {
    assertThat(HeaderUtil.checksumHeaderFrom(null, null)).isEmpty()
  }

  @Test
  fun `checksumHeaderFrom returns empty map when algorithm is null`() {
    assertThat(HeaderUtil.checksumHeaderFrom("abc123", null)).isEmpty()
  }

  @Test
  fun `checksumHeaderFrom returns empty map when checksum is null`() {
    assertThat(HeaderUtil.checksumHeaderFrom(null, ChecksumAlgorithm.SHA256)).isEmpty()
  }

  @Test
  fun `checksumHeaderFrom returns sha256 header entry`() {
    val result = HeaderUtil.checksumHeaderFrom("checksum-value", ChecksumAlgorithm.SHA256)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_SHA256, "checksum-value")
  }

  @Test
  fun `checksumHeaderFrom returns sha1 header entry`() {
    val result = HeaderUtil.checksumHeaderFrom("checksum-value", ChecksumAlgorithm.SHA1)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_SHA1, "checksum-value")
  }

  @Test
  fun `checksumHeaderFrom returns crc32 header entry`() {
    val result = HeaderUtil.checksumHeaderFrom("checksum-value", ChecksumAlgorithm.CRC32)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_CRC32, "checksum-value")
  }

  @Test
  fun `checksumHeaderFrom returns crc32c header entry`() {
    val result = HeaderUtil.checksumHeaderFrom("checksum-value", ChecksumAlgorithm.CRC32C)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_CRC32C, "checksum-value")
  }

  @Test
  fun `checksumHeaderFrom returns crc64nvme header entry`() {
    val result = HeaderUtil.checksumHeaderFrom("checksum-value", ChecksumAlgorithm.CRC64NVME)
    assertThat(result).containsEntry(X_AMZ_CHECKSUM_CRC64NVME, "checksum-value")
  }

  @Test
  fun `checksumAlgorithmFromHeader detects sha256`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_SHA256, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.SHA256)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects sha1`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_SHA1, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.SHA1)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects crc32`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC32, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC32)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects crc32c`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC32C, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC32C)
  }

  @Test
  fun `checksumAlgorithmFromHeader detects crc64nvme`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC64NVME, "value") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.CRC64NVME)
  }

  @Test
  fun `checksumAlgorithmFromHeader falls back to algorithm header`() {
    val headers = HttpHeaders().apply { set(AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM, "SHA256") }
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(headers)).isEqualTo(ChecksumAlgorithm.SHA256)
  }

  @Test
  fun `checksumAlgorithmFromHeader returns null when no checksum header present`() {
    assertThat(HeaderUtil.checksumAlgorithmFromHeader(HttpHeaders())).isNull()
  }

  @Test
  fun `checksumAlgorithmFromSdk returns algorithm when header present`() {
    val headers = HttpHeaders().apply { set(X_AMZ_SDK_CHECKSUM_ALGORITHM, "CRC32") }
    assertThat(HeaderUtil.checksumAlgorithmFromSdk(headers)).isEqualTo(ChecksumAlgorithm.CRC32)
  }

  @Test
  fun `checksumAlgorithmFromSdk returns null when header absent`() {
    assertThat(HeaderUtil.checksumAlgorithmFromSdk(HttpHeaders())).isNull()
  }

  @Test
  fun `checksumTypeFrom returns type when header present`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_TYPE, "FULL_OBJECT") }
    assertThat(HeaderUtil.checksumTypeFrom(headers)).isEqualTo(ChecksumType.FULL_OBJECT)
  }

  @Test
  fun `checksumTypeFrom returns null when header absent`() {
    assertThat(HeaderUtil.checksumTypeFrom(HttpHeaders())).isNull()
  }

  @Test
  fun `checksumFrom returns sha256 value`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_SHA256, "sha256-value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("sha256-value")
  }

  @Test
  fun `checksumFrom returns sha1 value`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_SHA1, "sha1-value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("sha1-value")
  }

  @Test
  fun `checksumFrom returns crc32 value`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC32, "crc32-value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc32-value")
  }

  @Test
  fun `checksumFrom returns crc32c value`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC32C, "crc32c-value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc32c-value")
  }

  @Test
  fun `checksumFrom returns crc64nvme value`() {
    val headers = HttpHeaders().apply { set(X_AMZ_CHECKSUM_CRC64NVME, "crc64nvme-value") }
    assertThat(HeaderUtil.checksumFrom(headers)).isEqualTo("crc64nvme-value")
  }

  @Test
  fun `checksumFrom returns null when no checksum header present`() {
    assertThat(HeaderUtil.checksumFrom(HttpHeaders())).isNull()
  }

  @Test
  fun `mapChecksumToHeader maps SHA256`() {
    assertThat(HeaderUtil.mapChecksumToHeader(ChecksumAlgorithm.SHA256)).isEqualTo(X_AMZ_CHECKSUM_SHA256)
  }

  @Test
  fun `mapChecksumToHeader maps SHA1`() {
    assertThat(HeaderUtil.mapChecksumToHeader(ChecksumAlgorithm.SHA1)).isEqualTo(X_AMZ_CHECKSUM_SHA1)
  }

  @Test
  fun `mapChecksumToHeader maps CRC32`() {
    assertThat(HeaderUtil.mapChecksumToHeader(ChecksumAlgorithm.CRC32)).isEqualTo(X_AMZ_CHECKSUM_CRC32)
  }

  @Test
  fun `mapChecksumToHeader maps CRC32C`() {
    assertThat(HeaderUtil.mapChecksumToHeader(ChecksumAlgorithm.CRC32C)).isEqualTo(X_AMZ_CHECKSUM_CRC32C)
  }

  @Test
  fun `mapChecksumToHeader maps CRC64NVME`() {
    assertThat(HeaderUtil.mapChecksumToHeader(ChecksumAlgorithm.CRC64NVME)).isEqualTo(X_AMZ_CHECKSUM_CRC64NVME)
  }

  @Test
  fun `storageClassHeaders returns empty map for STANDARD`() {
    val metadata = s3ObjectMetadata(storageClass = StorageClass.STANDARD)
    assertThat(metadata.storageClassHeaders()).isEmpty()
  }

  @Test
  fun `storageClassHeaders returns empty map when storageClass is null`() {
    val metadata = s3ObjectMetadata(storageClass = null)
    assertThat(metadata.storageClassHeaders()).isEmpty()
  }

  @Test
  fun `storageClassHeaders returns header for non-STANDARD storage class`() {
    val metadata = s3ObjectMetadata(storageClass = StorageClass.REDUCED_REDUNDANCY)
    val result = metadata.storageClassHeaders()
    assertThat(result).containsEntry(X_AMZ_STORAGE_CLASS, StorageClass.REDUCED_REDUNDANCY.toString())
  }

  @Test
  fun `versionHeader for S3ObjectMetadata returns empty map when versionId is null`() {
    val metadata = s3ObjectMetadata(versionId = null)
    assertThat(metadata.versionHeader(true)).isEmpty()
  }

  @Test
  fun `versionHeader for S3ObjectMetadata returns empty map when versioning disabled`() {
    val metadata = s3ObjectMetadata(versionId = "v1")
    assertThat(metadata.versionHeader(false)).isEmpty()
  }

  @Test
  fun `versionHeader for S3ObjectMetadata returns header when versioning enabled and versionId set`() {
    val metadata = s3ObjectMetadata(versionId = "v1")
    val result = metadata.versionHeader(true)
    assertThat(result).containsEntry(X_AMZ_VERSION_ID, "v1")
  }

  @Test
  fun `versionHeader for CopySource returns empty map when versionId is null`() {
    val copySource = CopySource("bucket", "key", null)
    assertThat(copySource.versionHeader(true)).isEmpty()
  }

  @Test
  fun `versionHeader for CopySource returns empty map when versioning disabled`() {
    val copySource = CopySource("bucket", "key", "v1")
    assertThat(copySource.versionHeader(false)).isEmpty()
  }

  @Test
  fun `versionHeader for CopySource returns header when versioning enabled and versionId set`() {
    val copySource = CopySource("bucket", "key", "v1")
    val result = copySource.versionHeader(true)
    assertThat(result).containsEntry(X_AMZ_COPY_SOURCE_VERSION_ID, "v1")
  }

  private fun s3ObjectMetadata(
    id: UUID = UUID.randomUUID(),
    key: String = "key",
    userMetadata: Map<String, String>? = null,
    storageClass: StorageClass? = StorageClass.STANDARD,
    versionId: String? = null,
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
