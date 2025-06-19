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

import java.io.IOException
import java.io.InputStream

/**
 * Merges chunks from AWS chunked AwsChunkedEncodingInputStream, skipping V4 style signing metadata.
 * The checksum is optionally included in the stream as part of the "trail headers"
 * after the last chunk.
 *
 * The original stream looks like this:
 *
 * <pre>
 * 24;chunk-signature=312a41de690364ad6d17629d1e026c448e78abd328f1602276fdd2c3f928d100
 * ## sample test file ##
 *
 * demo=content
 * 0;chunk-signature=4d2b448448f29b473beb81340f5a3d6c9468e4fbb9ac761cfab63846919011fb
 * x-amz-checksum-sha256:1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=
 * </pre>
 *
 * The format of each chunk of data is:
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * </pre>
 *
 * The format of the full payload is:
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * 0;chunk-signature=[sha256-signature][EOL]
 * x-amz-checksum-[checksum-algorithm]:[checksum][EOL]
 * [other trail headers]
 * </pre>
 *
 * @see  [AwsChunkedEncodingInputStream](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/internal/io/AwsChunkedEncodingInputStream.html)
 */
class AwsChunkedDecodingChecksumInputStream(source: InputStream, decodedLength: Long) : AbstractAwsInputStream(source, decodedLength) {
  @Throws(IOException::class)
  override fun read(): Int {
    if (chunkLength == 0L) {
      // try to read chunk length
      val hexLengthBytes = readHexLength()
      if (hexLengthBytes.isEmpty()) {
        return -1
      }

      setChunkLength(hexLengthBytes)

      if (chunkLength == 0L) {
        // chunk length found, but was "0". Try and find the checksum.
        extractAlgorithmAndChecksum()
        return -1
      }

      chunks++
      readUntil(CRLF)
    }

    readDecodedLength++
    chunkLength--

    return source.read()
  }

  @Throws(IOException::class)
  private fun readHexLength(): ByteArray {
    return readUntil(DELIMITER)
  }
}
