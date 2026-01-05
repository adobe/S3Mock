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
import kotlin.io.path.outputStream

abstract class ServiceBase {
  fun verifyChecksum(path: Path, checksum: String, checksumAlgorithm: ChecksumAlgorithm) {
    val computed = checksumFor(path, checksumAlgorithm.toChecksumAlgorithm())
    verifyChecksum(checksum, computed, checksumAlgorithm)
  }

  fun toTempFile(inputStream: InputStream, httpHeaders: HttpHeaders): Pair<Path, String?> {
    return try {
      val tempFile = Files.createTempFile("ObjectService", "toTempFile")
      tempFile.outputStream().use { os ->
        wrapStream(inputStream, httpHeaders).use { wrapped ->
          wrapped.transferTo(os)
          val algoFromSdk = checksumAlgorithmFromSdk(httpHeaders)
          if (algoFromSdk != null && wrapped is AbstractAwsInputStream) {
            tempFile to wrapped.checksum
          } else {
            tempFile to null
          }
        }
      }
    } catch (e: IOException) {
      LOG.error("Error reading from InputStream", e)
      throw S3Exception.BAD_REQUEST_CONTENT
    }
  }

  fun toTempFile(inputStream: InputStream): Pair<Path, String?> {
    return try {
      val tempFile = Files.createTempFile("ObjectService", "toTempFile")
      tempFile.outputStream().use {
        inputStream.transferTo(it)
      }
      tempFile to null
    } catch (e: IOException) {
      LOG.error("Error reading from InputStream", e)
      throw S3Exception.BAD_REQUEST_CONTENT
    }
  }

  private fun wrapStream(dataStream: InputStream, headers: HttpHeaders): InputStream {
    val length = headers.getFirst(X_AMZ_DECODED_CONTENT_LENGTH)?.toLong() ?: -1L
    return when {
      isV4Signed(headers) -> AwsChunkedDecodingChecksumInputStream(dataStream, length)
      isChunkedEncoding(headers) -> AwsUnsignedChunkedDecodingChecksumInputStream(dataStream, length)
      else -> dataStream
    }
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(ServiceBase::class.java)

    fun <T> mapContents(
      contents: List<T>,
      transformer: (T) -> T
    ): List<T> = contents.map(transformer)

    fun <T> filterBy(
      contents: List<T>,
      selector: (T) -> String?,
      compareTo: String?
    ): List<T> =
      compareTo?.let { threshold ->
        contents.filter { selector(it)?.let { candidate -> candidate > threshold } == true }
      } ?: contents

    fun <T> filterBy(
      contents: List<T>,
      selector: (T) -> Int,
      compareTo: Int?
    ): List<T> {
      return if (compareTo != null) {
        contents.filter { selector(it) > compareTo }
      } else {
        contents
      }
    }

    fun <T> filterBy(
      contents: List<T>,
      selector: (T) -> String?,
      prefixes: List<String>?
    ): List<T> {
      return if (!prefixes.isNullOrEmpty()) {
        contents.filter { content -> prefixes.none { prefix -> selector(content)?.startsWith(prefix) ?: false } }
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
      function: (T) -> String
    ): List<String> {
      if (delimiter.isNullOrEmpty()) return emptyList()

      val normalizedQueryPrefix = queryPrefix.orEmpty()
      val commonPrefixes = mutableListOf<String>()

      for (c in contents) {
        val key = function(c)
        if (key.startsWith(normalizedQueryPrefix)) {
          val delimiterIndex = key.indexOf(delimiter, startIndex = normalizedQueryPrefix.length)
          if (delimiterIndex > 0) {
            val commonPrefix = key.take(delimiterIndex + delimiter.length)
            if (commonPrefix !in commonPrefixes) {
              commonPrefixes += commonPrefix
            }
          }
        }
      }
      return commonPrefixes
    }
  }
}
