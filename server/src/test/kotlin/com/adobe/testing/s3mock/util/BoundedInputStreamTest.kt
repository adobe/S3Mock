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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class BoundedInputStreamTest {
  @Test
  fun `single byte read returns EOF after limit is reached`() {
    val data = byteArrayOf(1, 2, 3, 4, 5)
    val iut = BoundedInputStream(data.inputStream(), 3)

    assertThat(iut.read()).isEqualTo(1)
    assertThat(iut.read()).isEqualTo(2)
    assertThat(iut.read()).isEqualTo(3)
    // limit reached — next read must return -1
    assertThat(iut.read()).isEqualTo(-1)
  }

  @Test
  fun `bulk read stops at byte limit`() {
    val data = "hello world".toByteArray()
    val iut = BoundedInputStream(data.inputStream(), 5)

    val buf = ByteArray(10)
    val count = iut.read(buf, 0, 10)

    assertThat(count).isEqualTo(5)
    assertThat(buf.take(5).toByteArray().toString(Charsets.UTF_8)).isEqualTo("hello")
  }

  @Test
  fun `bulk read returns -1 when limit is already exhausted`() {
    val data = byteArrayOf(1, 2, 3)
    val iut = BoundedInputStream(data.inputStream(), 2)

    val first = ByteArray(2)
    iut.read(first, 0, 2)

    val second = ByteArray(2)
    val count = iut.read(second, 0, 2)

    assertThat(count).isEqualTo(-1)
  }

  @Test
  fun `zero limit prevents any bytes from being read`() {
    val data = byteArrayOf(1, 2, 3)
    val iut = BoundedInputStream(data.inputStream(), 0)

    assertThat(iut.read()).isEqualTo(-1)

    val buf = ByteArray(3)
    assertThat(iut.read(buf, 0, 3)).isEqualTo(-1)
  }

  @Test
  fun `limit larger than data reads entire stream`() {
    val data = byteArrayOf(10, 20, 30)
    val iut = BoundedInputStream(data.inputStream(), 100)

    val result = iut.readBytes()

    assertThat(result).containsExactly(10, 20, 30)
  }

  @Test
  fun `partial bulk read respects remaining byte budget`() {
    // limit = 4, ask for 3 twice — second read should only give 1 byte
    val data = byteArrayOf(1, 2, 3, 4, 5, 6)
    val iut = BoundedInputStream(data.inputStream(), 4)

    val buf = ByteArray(3)
    val first = iut.read(buf, 0, 3)
    assertThat(first).isEqualTo(3)

    val second = iut.read(buf, 0, 3)
    assertThat(second).isEqualTo(1)
    assertThat(buf[0]).isEqualTo(4)
  }
}
