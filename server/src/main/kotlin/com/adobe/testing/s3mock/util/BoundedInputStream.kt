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

import java.io.InputStream

/**
 * Implementation of [InputStream] that limits the number of bytes that can be read from the underlying
 */
class BoundedInputStream(
  private val input: InputStream,
  private val maxBytes: Long
) : InputStream() {
  private var bytesRead: Long = 0

  override fun read(): Int {
    if (bytesRead >= maxBytes) return -1
    val byte = input.read()
    if (byte != -1) bytesRead++
    return byte
  }

  override fun read(b: ByteArray, off: Int, len: Int): Int {
    if (bytesRead >= maxBytes) return -1
    val maxRead = minOf(len.toLong(), maxBytes - bytesRead).toInt()
    val count = input.read(b, off, maxRead)
    if (count != -1) bytesRead += count
    return count
  }
}
