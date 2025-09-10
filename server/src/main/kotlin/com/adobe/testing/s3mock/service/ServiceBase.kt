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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.util.AbstractAwsInputStream
import com.adobe.testing.s3mock.util.AwsChunkedDecodingChecksumInputStream
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DECODED_CONTENT_LENGTH
import com.adobe.testing.s3mock.util.AwsUnsignedChunkedDecodingChecksumInputStream
import com.adobe.testing.s3mock.util.DigestUtil.checksumFor
import com.adobe.testing.s3mock.util.DigestUtil.verifyChecksum
import com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk
import com.adobe.testing.s3mock.util.HeaderUtil.isChunkedEncoding
import com.adobe.testing.s3mock.util.HeaderUtil.isV4Signed
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Function
import java.util.function.UnaryOperator

abstract class ServiceBase {
    fun verifyChecksum(path: Path, checksum: String, checksumAlgorithm: ChecksumAlgorithm) {
        val checksumFor = checksumFor(path, checksumAlgorithm.toChecksumAlgorithm())
        verifyChecksum(checksum, checksumFor, checksumAlgorithm)
    }

    fun toTempFile(inputStream: InputStream, httpHeaders: HttpHeaders): Pair<Path, String?> {
        try {
            val tempFile = Files.createTempFile("ObjectService", "toTempFile")
            Files.newOutputStream(tempFile).use { os ->
                wrapStream(inputStream, httpHeaders).use { wrappedStream ->
                    wrappedStream.transferTo(os)
                    val algorithmFromSdk = checksumAlgorithmFromSdk(httpHeaders)
                    if (algorithmFromSdk != null
                        && wrappedStream is AbstractAwsInputStream
                    ) {
                        return Pair(tempFile, wrappedStream.checksum)
                    }
                    return Pair(tempFile, null)
                }
            }
        } catch (e: IOException) {
            LOG.error("Error reading from InputStream", e)
            throw S3Exception.BAD_REQUEST_CONTENT
        }
    }

    fun toTempFile(inputStream: InputStream): Pair<Path, String?> {
        try {
            val tempFile = Files.createTempFile("ObjectService", "toTempFile")
            Files.newOutputStream(tempFile).use { os ->
                inputStream.transferTo(os)
                return Pair(tempFile, null)
            }
        } catch (e: IOException) {
            LOG.error("Error reading from InputStream", e)
            throw S3Exception.BAD_REQUEST_CONTENT
        }
    }

    private fun wrapStream(dataStream: InputStream, headers: HttpHeaders): InputStream {
        val lengthHeader = headers.getFirst(X_AMZ_DECODED_CONTENT_LENGTH)
        val length = lengthHeader?.toLong() ?: -1
      return if (isV4Signed(headers)) {
        AwsChunkedDecodingChecksumInputStream(dataStream, length)
      } else if (isChunkedEncoding(headers)) {
        AwsUnsignedChunkedDecodingChecksumInputStream(dataStream, length)
      } else {
        dataStream
      }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(ServiceBase::class.java)

        fun <T> mapContents(
            contents: List<T>,
            extractor: UnaryOperator<T>
        ): List<T> {
            return contents
                .stream()
                .map<T>(extractor)
                .toList()
        }

        fun <T> filterBy(
            contents: List<T>,
            function: Function<T, String>,
            compareTo: String?
        ): List<T> {
          return if (!compareTo.isNullOrEmpty()) {
            contents
              .stream()
              .filter { content: T -> function.apply(content) > compareTo }
              .toList()
          } else {
            contents
          }
        }

        fun <T> filterBy(
            contents: List<T>,
            function: Function<T, Int>,
            compareTo: Int?
        ): List<T> {
          return if (compareTo != null) {
            contents
              .stream()
              .filter { content: T -> function.apply(content) > compareTo }
              .toList()
          } else {
            contents
          }
        }

        fun <T> filterBy(
            contents: List<T>,
            function: Function<T, String>,
            prefixes: List<String>?
        ): List<T> {
          return if (prefixes != null && !prefixes.isEmpty()) {
            contents
              .stream()
              .filter { content: T ->
                prefixes
                  .stream()
                  .noneMatch { prefix: String? -> function.apply(content).startsWith(prefix!!) }
              }
              .toList()
          } else {
            contents
          }
        }

        /**
         * Collapse all elements with keys starting with some prefix up to the given delimiter into
         * one prefix entry. Collapsed elements are removed from the contents list.
         *
         * @param queryPrefix the key prefix as specified in the list request
         * @param delimiter the delimiter used to separate a prefix from the rest of the object name
         * @param contents the list of contents to use for collapsing the prefixes
         * @param function the function to apply on a content to extract the value to collapse the prefixes on
         */
        fun <T> collapseCommonPrefixes(
            queryPrefix: String?,
            delimiter: String?,
            contents: List<T>,
            function: Function<T, String>
        ): List<String> {
            val commonPrefixes = mutableListOf<String>()
            if (delimiter.isNullOrEmpty()) {
                return commonPrefixes
            }

            val normalizedQueryPrefix = queryPrefix ?: ""

            for (c in contents) {
                val key = function.apply(c)
                if (key.startsWith(normalizedQueryPrefix)) {
                    val delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length)
                    if (delimiterIndex > 0) {
                        val commonPrefix = key.take(delimiterIndex + delimiter.length)
                        if (!commonPrefixes.contains(commonPrefix)) {
                            commonPrefixes.add(commonPrefix)
                        }
                    }
                }
            }
            return commonPrefixes
        }
    }
}
