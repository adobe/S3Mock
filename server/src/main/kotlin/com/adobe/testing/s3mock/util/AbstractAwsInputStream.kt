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
import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

abstract class AbstractAwsInputStream protected constructor(source: InputStream, val decodedLength: Long) : InputStream() {
  var readDecodedLength: Long = 0L
    protected set
  var checksum: String? = null
    protected set
  var algorithm: ChecksumAlgorithm? = null
    protected set
  var chunks: Int = 0
    protected set
  protected val source: InputStream = BufferedInputStream(source)
  protected var chunkLength: Long = 0L
  private val byteBuffer: ByteBuffer = ByteBuffer.allocate(MAX_CHUNK_SIZE)

  @Throws(IOException::class)
  override fun close() {
    source.close()
  }

  /**
   * Reads this stream until the byte sequence was found.
   *
   * @param endSequence The byte sequence to look for in the stream. The source stream is read
   * until the last bytes read are equal to this sequence.
   *
   * @return The bytes read *before* the end sequence started.
   */
  @Throws(IOException::class)
  protected fun readUntil(endSequence: ByteArray): ByteArray {
    byteBuffer.clear()
    while (!endsWith(byteBuffer.asReadOnlyBuffer(), endSequence)) {
      val c = source.read()
      if (c < 0) {
        return ByteArray(0)
      }

      val unsigned = (c and 0xFF).toByte()
      byteBuffer.put(unsigned)
    }

    val result = ByteArray(byteBuffer.position() - endSequence.size)
    byteBuffer.rewind()
    byteBuffer.get(result)
    return result
  }

  protected fun endsWith(buffer: ByteBuffer, endSequence: ByteArray): Boolean {
    val pos = buffer.position()
    if (pos >= endSequence.size) {
      for (i in endSequence.indices) {
        if (buffer.get(pos - endSequence.size + i) != endSequence[i]) {
          return false
        }
      }

      return true
    }

    return false
  }

  protected fun setChunkLength(hexLengthBytes: ByteArray) {
    chunkLength = String(hexLengthBytes, StandardCharsets.UTF_8).trim { it <= ' ' }.toLong(16)
  }

  @Throws(IOException::class)
  protected fun extractAlgorithmAndChecksum() {
    if (algorithm == null && checksum == null) {
      readUntil(CHECKSUM_HEADER)
      val typeAndChecksum = readUntil(CRLF)
      val typeAndChecksumString = String(typeAndChecksum)
      if (!typeAndChecksumString.isBlank()) {
        val split: Array<String?> = typeAndChecksumString.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val type: String = split[0]!!
        algorithm = ChecksumAlgorithm.fromString(type)
        checksum = split[1]
      }
    }
  }

  companion object {
    @JvmStatic
    protected val CRLF: ByteArray = "\r\n".toByteArray(StandardCharsets.UTF_8)
    @JvmStatic
    protected val DELIMITER: ByteArray = ";".toByteArray(StandardCharsets.UTF_8)
    protected val CHECKSUM_HEADER: ByteArray = "x-amz-checksum-".toByteArray(StandardCharsets.UTF_8)

    /**
     * That's the max chunk buffer size used in the AWS implementation.
     */
    private const val MAX_CHUNK_SIZE = 256 * 1024
  }
}
