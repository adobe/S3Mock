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

class BoundedInputStreamTest {
  @Test
  fun `read single byte returns -1 when bound is reached`() {
    val data = byteArrayOf(1, 2, 3, 4, 5)
    val bounded = BoundedInputStream(data.inputStream(), 3)

    assertThat(bounded.read()).isEqualTo(1)
    assertThat(bounded.read()).isEqualTo(2)
    assertThat(bounded.read()).isEqualTo(3)
    assertThat(bounded.read()).isEqualTo(-1)
  }

  @Test
  fun `read bulk returns -1 when bound is reached`() {
    val data = byteArrayOf(10, 20, 30, 40, 50)
    val bounded = BoundedInputStream(data.inputStream(), 3)
    val buf = ByteArray(10)

    val n = bounded.read(buf, 0, buf.size)
    assertThat(n).isEqualTo(3)
    assertThat(buf.take(3)).containsExactly(10, 20, 30)

    assertThat(bounded.read(buf, 0, buf.size)).isEqualTo(-1)
  }

  @Test
  fun `read bulk limits to maxBytes even when len is larger`() {
    val data = byteArrayOf(1, 2, 3, 4, 5)
    val bounded = BoundedInputStream(data.inputStream(), 2)
    val buf = ByteArray(5)

    val n = bounded.read(buf, 0, buf.size)
    assertThat(n).isEqualTo(2)
    assertThat(buf[0]).isEqualTo(1)
    assertThat(buf[1]).isEqualTo(2)
  }

  @Test
  fun `read stops at underlying stream EOF before bound is reached`() {
    val data = byteArrayOf(1, 2)
    val bounded = BoundedInputStream(data.inputStream(), 100)
    val buf = ByteArray(10)

    val n = bounded.read(buf, 0, buf.size)
    assertThat(n).isEqualTo(2)

    assertThat(bounded.read(buf, 0, buf.size)).isEqualTo(-1)
  }

  @Test
  fun `zero maxBytes returns -1 immediately for single byte read`() {
    val data = byteArrayOf(1, 2, 3)
    val bounded = BoundedInputStream(data.inputStream(), 0)

    assertThat(bounded.read()).isEqualTo(-1)
  }

  @Test
  fun `zero maxBytes returns -1 immediately for bulk read`() {
    val data = byteArrayOf(1, 2, 3)
    val bounded = BoundedInputStream(data.inputStream(), 0)
    val buf = ByteArray(10)

    assertThat(bounded.read(buf, 0, buf.size)).isEqualTo(-1)
  }
}
