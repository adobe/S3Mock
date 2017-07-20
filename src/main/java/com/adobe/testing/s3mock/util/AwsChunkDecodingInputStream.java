/*
 *  Copyright 2017 Adobe.
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Skips V4 style signing metadata from input streams.
 * <p>
 * The original stream looks like this (newlines are CRLF):
 *
 * <pre>
 * 5;chunk-signature=7ece820edcf094ce1ef6d643c8db60b67913e28831d9b0430efd2b56a9deec5e
 * 12345
 * 0;chunk-signature=ee2c094d7162170fcac17d2c76073cd834b0488bfe52e89e48599b8115c7ffa2
 * </pre>
 *
 * The format of each chunk of data is:
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][crlf]
 * [payload-bytes-of-this-chunk][crlf]
 * </pre>
 *
 * @see com.amazonaws.auth.AwsChunkedEncodingInputStream
 */
public class AwsChunkDecodingInputStream extends InputStream {
  /**
   * That's the max chunk buffer size used in the AWS implementation.
   */
  private static final int MAX_CHUNK_SIZE = 256 * 1024;

  private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

  private static final byte[] DELIMITER = ";".getBytes(StandardCharsets.UTF_8);

  private final InputStream source;

  private int remainingInChunk = 0;

  private final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_CHUNK_SIZE);

  /**
   * Constructs a new {@link AwsChunkDecodingInputStream}.
   *
   * @param source The {@link InputStream} to wrap.
   */
  public AwsChunkDecodingInputStream(final InputStream source) {
    this.source = source;
  }

  @Override
  public int read() throws IOException {
    if (remainingInChunk == 0) {
      final byte[] hexLengthBytes = readUntil(DELIMITER);
      if (hexLengthBytes == null) {
        return -1;
      }

      remainingInChunk =
          Integer.parseInt(new String(hexLengthBytes, StandardCharsets.UTF_8).trim(), 16);

      if (remainingInChunk == 0) {
        return -1;
      }

      readUntil(CRLF);
    }

    remainingInChunk--;

    return source.read();
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  /**
   * @param endSequence The byte sequence to look for in the stream. The source stream is read
   *                    until the last bytes
   *            read are equal to this sequence.
   * @return The bytes read <em>before</em> the end sequence started.
   */
  private byte[] readUntil(final byte[] endSequence) throws IOException {
    byteBuffer.clear();
    while (!endsWith(byteBuffer.asReadOnlyBuffer(), endSequence)) {
      final int c = source.read();
      if (c < 0) {
        return null;
      }

      final byte unsigned = (byte) (c & 0xFF);
      byteBuffer.put(unsigned);
    }

    final byte[] result = new byte[byteBuffer.position() - endSequence.length];
    byteBuffer.rewind();
    byteBuffer.get(result);
    return result;
  }

  private boolean endsWith(final ByteBuffer buffer, final byte[] endSequence) {
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
}
