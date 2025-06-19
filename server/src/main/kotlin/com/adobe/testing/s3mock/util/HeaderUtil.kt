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
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders.AWS_CHUNKED
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32C
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC64NVME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA1
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import org.apache.commons.lang3.StringUtils
import org.springframework.http.HttpHeaders
import org.springframework.http.InvalidMediaTypeException
import org.springframework.http.MediaType
import java.util.function.Predicate

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
     * Creates response headers from S3ObjectMetadata user metadata.
     * @param s3ObjectMetadata [com.adobe.testing.s3mock.store.S3ObjectMetadata] S3Object where user metadata will be extracted
     */
    @JvmStatic
    fun userMetadataHeadersFrom(s3ObjectMetadata: S3ObjectMetadata): Map<String, String> {
        val metadataHeaders = HashMap<String, String>()
        if (s3ObjectMetadata.userMetadata != null) {
            s3ObjectMetadata.userMetadata!!
                .forEach { (key: String?, value: String?) ->
                    if (StringUtils.startsWithIgnoreCase(key, HEADER_X_AMZ_META_PREFIX)) {
                        metadataHeaders.put(key!!, value!!)
                    } else {
                        //support case where metadata was stored locally in legacy format
                        metadataHeaders.put(HEADER_X_AMZ_META_PREFIX + key, value!!)
                    }
                }
        }
        return metadataHeaders
    }

    /**
     * Creates response headers from S3ObjectMetadata storageclass.
     * @param s3ObjectMetadata [S3ObjectMetadata] S3Object where data will be extracted
     */
    @JvmStatic
    fun storageClassHeadersFrom(s3ObjectMetadata: S3ObjectMetadata): Map<String, String> {
        val headers = HashMap<String, String>()
        val storageClass = s3ObjectMetadata.storageClass
        if (storageClass != null) {
            headers.put(X_AMZ_STORAGE_CLASS, storageClass.toString())
        }
        return headers
    }

    /**
     * Retrieves user metadata from request.
     * @param headers [org.springframework.http.HttpHeaders]
     * @return map containing user meta-data
     */
    @JvmStatic
    fun userMetadataFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(
            headers,
            Predicate { header: String -> StringUtils.startsWithIgnoreCase(header, HEADER_X_AMZ_META_PREFIX) })
    }

    /**
     * Retrieves headers to store from request.
     * @param headers [HttpHeaders]
     * @return map containing headers to store
     */
    @JvmStatic
    fun storeHeadersFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(
            headers
        ) { header: String ->
            (StringUtils.equalsIgnoreCase(header, HttpHeaders.EXPIRES)
                    || StringUtils.equalsIgnoreCase(header, HttpHeaders.CONTENT_LANGUAGE)
                    || StringUtils.equalsIgnoreCase(header, HttpHeaders.CONTENT_DISPOSITION)
                    || (StringUtils.equalsIgnoreCase(header, HttpHeaders.CONTENT_ENCODING)
                    && !isOnlyChunkedEncoding(headers))
                    || StringUtils.equalsIgnoreCase(header, HttpHeaders.CACHE_CONTROL)
                    )
        }
    }

    /**
     * Retrieves headers encryption headers from request.
     * @param headers [HttpHeaders]
     * @return map containing encryption headers
     */
    @JvmStatic
    fun encryptionHeadersFrom(headers: HttpHeaders): Map<String, String> {
        return parseHeadersToMap(
            headers
        ) { header: String -> StringUtils.startsWithIgnoreCase(header, X_AMZ_SERVER_SIDE_ENCRYPTION) }
    }

    private fun parseHeadersToMap(
        headers: HttpHeaders,
        matcher: Predicate<String>
    ): Map<String, String> {
        return headers
            .headerSet()
            .map {
                if (matcher.test(it.key)
                    && !it.value.isEmpty() && it.value[0].isNotBlank()
                ) {
                    return@map it.key to it.value[0]
                } else {
                    return@map null
                }
            }
            .filterNotNull()
            .associate { it.first to it.second }
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
        val contentEncodingHeaders: List<String?>? = headers.get(HttpHeaders.CONTENT_ENCODING)
        return (contentEncodingHeaders != null
                && (contentEncodingHeaders.contains(AWS_CHUNKED)))
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
        val contentEncodingHeaders: List<String?>? = headers.get(HttpHeaders.CONTENT_ENCODING)
        return (contentEncodingHeaders != null && contentEncodingHeaders.size == 1 && (contentEncodingHeaders.contains(
            AWS_CHUNKED
        )))
    }

    @JvmStatic
    fun mediaTypeFrom(contentType: String?): MediaType {
        return if (contentType == null) {
            FALLBACK_MEDIA_TYPE
        } else {
            try {
                MediaType.parseMediaType(contentType)
            } catch (e: InvalidMediaTypeException) {
                FALLBACK_MEDIA_TYPE
            }
        }
    }

    @JvmStatic
    fun overrideHeadersFrom(queryParams: Map<String, String>): Map<String, String> {
        return queryParams
            .entries
            .map {
                if (mapHeaderName(it.key).isNotBlank()) {
                    return@map mapHeaderName(it.key) to it.value
                } else {
                    return@map null
                }
            }
            .filterNotNull()
            .associate { it.first to it.second }
    }


    @JvmStatic
    fun checksumHeaderFrom(s3ObjectMetadata: S3ObjectMetadata): MutableMap<String, String> {
        val checksumAlgorithm = s3ObjectMetadata.checksumAlgorithm
        val checksum = s3ObjectMetadata.checksum
        return checksumHeaderFrom(checksum, checksumAlgorithm)
    }

    @JvmStatic
    fun checksumHeaderFrom(
        checksum: String?,
        checksumAlgorithm: ChecksumAlgorithm?
    ): MutableMap<String, String> {
        val headers = HashMap<String, String>()
        if (checksumAlgorithm != null && checksum != null) {
            headers.put(mapChecksumToHeader(checksumAlgorithm), checksum)
        }
        return headers
    }

    @JvmStatic
    fun checksumAlgorithmFromHeader(headers: HttpHeaders): ChecksumAlgorithm? {
        if (headers.containsHeader(X_AMZ_CHECKSUM_SHA256)) {
            return ChecksumAlgorithm.SHA256
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_SHA1)) {
            return ChecksumAlgorithm.SHA1
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32)) {
            return ChecksumAlgorithm.CRC32
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32C)) {
            return ChecksumAlgorithm.CRC32C
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME)) {
            return ChecksumAlgorithm.CRC64NVME
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_ALGORITHM)) {
            val checksumAlgorithm = headers.getFirst(X_AMZ_CHECKSUM_ALGORITHM)
            return ChecksumAlgorithm.fromString(checksumAlgorithm)
        } else {
            return null
        }
    }

    @JvmStatic
    fun checksumAlgorithmFromSdk(headers: HttpHeaders): ChecksumAlgorithm? {
        if (headers.containsHeader(X_AMZ_SDK_CHECKSUM_ALGORITHM)) {
            return ChecksumAlgorithm.fromString(headers.getFirst(X_AMZ_SDK_CHECKSUM_ALGORITHM))
        } else {
            return null
        }
    }

    @JvmStatic
    fun checksumFrom(headers: HttpHeaders): String? {
        if (headers.containsHeader(X_AMZ_CHECKSUM_SHA256)) {
            return headers.getFirst(X_AMZ_CHECKSUM_SHA256)
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_SHA1)) {
            return headers.getFirst(X_AMZ_CHECKSUM_SHA1)
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32)) {
            return headers.getFirst(X_AMZ_CHECKSUM_CRC32)
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32C)) {
            return headers.getFirst(X_AMZ_CHECKSUM_CRC32C)
        } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME)) {
            return headers.getFirst(X_AMZ_CHECKSUM_CRC64NVME)
        }
        return null
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
