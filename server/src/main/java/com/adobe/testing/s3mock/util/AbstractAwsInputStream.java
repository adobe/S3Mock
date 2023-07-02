/*
 *  Copyright 2017-2023 Adobe.
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

package com.adobe.testing.s3mock.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

abstract class AbstractAwsInputStream extends InputStream {
  protected static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
  protected static final byte[] DELIMITER = ";".getBytes(StandardCharsets.UTF_8);
  protected final InputStream source;
  protected long payloadLength = 0L;
  /**
   * That's the max chunk buffer size used in the AWS implementation.
   */
  private static final int MAX_CHUNK_SIZE = 256 * 1024;
  private final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_CHUNK_SIZE);

  protected AbstractAwsInputStream(final InputStream source) {
    this.source = new BufferedInputStream(source);
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  /**
   * Reads this stream until the byte sequence was found.
   *
   * @param endSequence The byte sequence to look for in the stream. The source stream is read
   *     until the last bytes read are equal to this sequence.
   *
   * @return The bytes read <em>before</em> the end sequence started.
   */
  protected byte[] readUntil(final byte[] endSequence) throws IOException {
    byteBuffer.clear();
    while (!endsWith(byteBuffer.asReadOnlyBuffer(), endSequence)) {
      final int c = source.read();
      if (c < 0) {
        return new byte[0];
      }

      final byte unsigned = (byte) (c & 0xFF);
      byteBuffer.put(unsigned);
    }

    final byte[] result = new byte[byteBuffer.position() - endSequence.length];
    byteBuffer.rewind();
    byteBuffer.get(result);
    return result;
  }

  protected boolean endsWith(final ByteBuffer buffer, final byte[] endSequence) {
    final int pos = buffer.position();
    if (pos >= endSequence.length) {
      for (int i = 0; i < endSequence.length; i++) {
        if (buffer.get(pos - endSequence.length + i) != endSequence[i]) {
          return false;
        }
      }

      return true;
    }

    return false;
  }

  protected void setPayloadLength(byte[] hexLengthBytes) {
    payloadLength = Long.parseLong(new String(hexLengthBytes, StandardCharsets.UTF_8).trim(), 16);
  }
}
