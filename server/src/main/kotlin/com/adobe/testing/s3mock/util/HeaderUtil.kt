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
import com.adobe.testing.s3mock.dto.CopySource
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders.AWS_CHUNKED
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM
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
import com.adobe.testing.s3mock.util.HeaderUtil.HEADER_X_AMZ_META_PREFIX
import com.adobe.testing.s3mock.util.HeaderUtil.checksumHeaderFrom
import org.springframework.http.HttpHeaders
import org.springframework.http.InvalidMediaTypeException
import org.springframework.http.MediaType

fun CopySource.versionHeader(versioning: Boolean): Map<String, String> {
  return if (versioning && versionId != null && !versionId.isEmpty()) {
    mapOf(X_AMZ_COPY_SOURCE_VERSION_ID to versionId)
  } else {
    emptyMap()
  }
}

fun S3ObjectMetadata.versionHeader(versioning: Boolean): Map<String, String> {
  return if (versioning && versionId != null && !versionId.isEmpty()) {
    mapOf(X_AMZ_VERSION_ID to versionId)
  } else {
    emptyMap()
  }
}

fun S3ObjectMetadata.checksumHeader(): Map<String, String> {
  val checksumAlgorithm = this.checksumAlgorithm
  val checksum = this.checksum
  return checksumHeaderFrom(checksum, checksumAlgorithm)
}

/**
 * Creates response headers from S3ObjectMetadata StorageClass.
 */
fun S3ObjectMetadata.storageClassHeaders(): Map<String, String> {
  val storageClass = this.storageClass ?: return emptyMap()
  if(storageClass == StorageClass.STANDARD) return emptyMap()
  return mapOf(X_AMZ_STORAGE_CLASS to storageClass.toString())
}

/**
 * Creates response headers from S3ObjectMetadata user metadata.
 */
fun S3ObjectMetadata.userMetadataHeaders(): Map<String, String> {
  val user = this.userMetadata.orEmpty()
  return buildMap {
    user.forEach { (k, v) ->
      if (k.isNotBlank() && v.isNotBlank()) {
        val key = if (k.startsWith(HEADER_X_AMZ_META_PREFIX, ignoreCase = true)) k else HEADER_X_AMZ_META_PREFIX + k
        put(key, v)
      }
    }
  }
}

object HeaderUtil {
    const val HEADER_X_AMZ_META_PREFIX: String = "x-amz-meta-"
    private const val RESPONSE_HEADER_CONTENT_TYPE = "response-content-type"
    private const val RESPONSE_HEADER_CONTENT_LANGUAGE = "response-content-language"
    private const val RESPONSE_HEADER_EXPIRES = "response-expires"
    private const val RESPONSE_HEADER_CACHE_CONTROL = "response-cache-control"
    private const val RESPONSE_HEADER_CONTENT_DISPOSITION = "response-content-disposition"
    private const val RESPONSE_HEADER_CONTENT_ENCODING = "response-content-encoding"
    private const val STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    private const val STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD_TRAILER = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    private val FALLBACK_MEDIA_TYPE = MediaType.APPLICATION_OCTET_STREAM

    /**
     * Retrieves user metadata from request.
     * @param headers [org.springframework.http.HttpHeaders]
     * @return map containing user meta-data
     */
    @JvmStatic
    fun userMetadataFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(headers) { header: String ->
            header.startsWith(HEADER_X_AMZ_META_PREFIX, ignoreCase = true)
        }
    }

    /**
     * Retrieves headers to store from request.
     * @param headers [HttpHeaders]
     * @return map containing headers to store
     */
    @JvmStatic
    fun storeHeadersFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(headers) { header: String ->
            header.equals(HttpHeaders.EXPIRES, ignoreCase = true)
                || header.equals(HttpHeaders.CONTENT_LANGUAGE, ignoreCase = true)
                || header.equals(HttpHeaders.CONTENT_DISPOSITION, ignoreCase = true)
                || (header.equals(HttpHeaders.CONTENT_ENCODING, ignoreCase = true) && !isOnlyChunkedEncoding(headers))
                || header.equals(HttpHeaders.CACHE_CONTROL, ignoreCase = true)
        }
    }

    /**
     * Retrieves headers encryption headers from request.
     * @param headers [HttpHeaders]
     * @return map containing encryption headers
     */
    @JvmStatic
    fun encryptionHeadersFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(headers) { header: String ->
            header.startsWith(X_AMZ_SERVER_SIDE_ENCRYPTION, ignoreCase = true)
        }
    }

    private fun parseHeadersToMap(
        headers: HttpHeaders,
        matcher: (String) -> Boolean
    ): Map<String, String> {
        return headers
            .headerSet()
            .mapNotNull { (key, values) ->
                val first = values.firstOrNull()
                if (matcher(key) && !first.isNullOrBlank()) key to first else null
            }
            .toMap()
    }

    @JvmStatic
    fun isV4Signed(headers: HttpHeaders): Boolean {
        val sha256Header = headers.getFirst(X_AMZ_CONTENT_SHA256)
        return sha256Header != null
                && (sha256Header == STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD
                || sha256Header == STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD_TRAILER)
    }

    @JvmStatic
    fun isChunkedEncoding(headers: HttpHeaders): Boolean {
        val contentEncodingHeaders: List<String?>? = headers[HttpHeaders.CONTENT_ENCODING]
        return contentEncodingHeaders?.contains(AWS_CHUNKED) == true
    }

    /**
     * Check if aws-chunked is the only "Content-Encoding" header.
     * <quote>
     * If aws-chunked is the only value that you pass in the content-encoding header, S3 considers
     * the content-encoding header empty and does not return this header when your retrieve the
     * object.
    </quote> *
     * See [API](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html)
     */
    private fun isOnlyChunkedEncoding(headers: HttpHeaders): Boolean {
        val contentEncodingHeaders: List<String?>? = headers[HttpHeaders.CONTENT_ENCODING]
        return contentEncodingHeaders?.size == 1 && contentEncodingHeaders.contains(AWS_CHUNKED)
    }

    @JvmStatic
    fun mediaTypeFrom(contentType: String?): MediaType {
        return contentType?.let {
            try {
                MediaType.parseMediaType(it)
            } catch (_: InvalidMediaTypeException) {
                FALLBACK_MEDIA_TYPE
            }
        } ?: FALLBACK_MEDIA_TYPE
    }

    @JvmStatic
    fun overrideHeadersFrom(queryParams: Map<String, String>): Map<String, String> {
        return queryParams.entries
            .mapNotNull { (k, v) ->
                val mapped = mapHeaderName(k)
                if (mapped.isNotBlank()) mapped to v else null
            }
            .toMap()
    }

    @JvmStatic
    fun checksumHeaderFrom(
        checksum: String?,
        checksumAlgorithm: ChecksumAlgorithm?
    ): Map<String, String> {
        return if (checksum != null && checksumAlgorithm != null) {
            mapOf(mapChecksumToHeader(checksumAlgorithm) to checksum)
        } else {
            mapOf()
        }
    }

    @JvmStatic
    fun checksumAlgorithmFromHeader(headers: HttpHeaders): ChecksumAlgorithm? {
        return when {
            headers.containsHeader(X_AMZ_CHECKSUM_SHA256) -> ChecksumAlgorithm.SHA256
            headers.containsHeader(X_AMZ_CHECKSUM_SHA1) -> ChecksumAlgorithm.SHA1
            headers.containsHeader(X_AMZ_CHECKSUM_CRC32) -> ChecksumAlgorithm.CRC32
            headers.containsHeader(X_AMZ_CHECKSUM_CRC32C) -> ChecksumAlgorithm.CRC32C
            headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME) -> ChecksumAlgorithm.CRC64NVME
            headers.containsHeader(X_AMZ_CHECKSUM_ALGORITHM) -> ChecksumAlgorithm.fromString(headers.getFirst(X_AMZ_CHECKSUM_ALGORITHM))
            else -> null
        }
    }

    @JvmStatic
    fun checksumAlgorithmFromSdk(headers: HttpHeaders): ChecksumAlgorithm? {
        return if (headers.containsHeader(X_AMZ_SDK_CHECKSUM_ALGORITHM))
            ChecksumAlgorithm.fromString(headers.getFirst(X_AMZ_SDK_CHECKSUM_ALGORITHM))
        else null
    }

    @JvmStatic
    fun checksumTypeFrom(headers: HttpHeaders): ChecksumType? {
        return if (headers.containsHeader(X_AMZ_CHECKSUM_TYPE))
            ChecksumType.fromString(headers.getFirst(X_AMZ_CHECKSUM_TYPE))
        else null
    }

    @JvmStatic
    fun checksumFrom(headers: HttpHeaders): String? {
        return when {
            headers.containsHeader(X_AMZ_CHECKSUM_SHA256) -> headers.getFirst(X_AMZ_CHECKSUM_SHA256)
            headers.containsHeader(X_AMZ_CHECKSUM_SHA1) -> headers.getFirst(X_AMZ_CHECKSUM_SHA1)
            headers.containsHeader(X_AMZ_CHECKSUM_CRC32) -> headers.getFirst(X_AMZ_CHECKSUM_CRC32)
            headers.containsHeader(X_AMZ_CHECKSUM_CRC32C) -> headers.getFirst(X_AMZ_CHECKSUM_CRC32C)
            headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME) -> headers.getFirst(X_AMZ_CHECKSUM_CRC64NVME)
            else -> null
        }
    }

    fun mapChecksumToHeader(checksumAlgorithm: ChecksumAlgorithm): String {
        return when (checksumAlgorithm) {
            ChecksumAlgorithm.SHA256 -> X_AMZ_CHECKSUM_SHA256
            ChecksumAlgorithm.SHA1 -> X_AMZ_CHECKSUM_SHA1
            ChecksumAlgorithm.CRC32 -> X_AMZ_CHECKSUM_CRC32
            ChecksumAlgorithm.CRC32C -> X_AMZ_CHECKSUM_CRC32C
            ChecksumAlgorithm.CRC64NVME -> X_AMZ_CHECKSUM_CRC64NVME
        }
    }

    private fun mapHeaderName(name: String): String {
        return when (name) {
            RESPONSE_HEADER_CACHE_CONTROL -> HttpHeaders.CACHE_CONTROL
            RESPONSE_HEADER_CONTENT_DISPOSITION -> HttpHeaders.CONTENT_DISPOSITION
            RESPONSE_HEADER_CONTENT_ENCODING -> HttpHeaders.CONTENT_ENCODING
            RESPONSE_HEADER_CONTENT_LANGUAGE -> HttpHeaders.CONTENT_LANGUAGE
            RESPONSE_HEADER_CONTENT_TYPE -> HttpHeaders.CONTENT_TYPE
            RESPONSE_HEADER_EXPIRES -> HttpHeaders.EXPIRES
            else -> ""
        }
    }
}
