/*
 *  Copyright 2017-2024 Adobe.
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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class AbstractAwsInputStream extends InputStream {
  protected static final byte[] CRLF = "\r\n".getBytes(UTF_8);
  protected static final byte[] DELIMITER = ";".getBytes(UTF_8);
  protected static final byte[] CHECKSUM_HEADER = "x-amz-checksum-".getBytes(UTF_8);
  protected long readDecodedLength = 0L;
  protected final InputStream source;
  protected long chunkLength = 0L;
  protected String checksum;
  protected ChecksumAlgorithm algorithm;
  /**
   * That's the max chunk buffer size used in the AWS implementation.
   */
  private static final int MAX_CHUNK_SIZE = 256 * 1024;
  private final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_CHUNK_SIZE);
  protected final long decodedLength;

  protected AbstractAwsInputStream(InputStream source, long decodedLength) {
    this.source = new BufferedInputStream(source);
    this.decodedLength = decodedLength;
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
      var c = source.read();
      if (c < 0) {
        return new byte[0];
      }

      var unsigned = (byte) (c & 0xFF);
      byteBuffer.put(unsigned);
    }

    var result = new byte[byteBuffer.position() - endSequence.length];
    byteBuffer.rewind();
    byteBuffer.get(result);
    return result;
  }

  protected boolean endsWith(final ByteBuffer buffer, final byte[] endSequence) {
    var pos = buffer.position();
    if (pos >= endSequence.length) {
      for (var i = 0; i < endSequence.length; i++) {
        if (buffer.get(pos - endSequence.length + i) != endSequence[i]) {
          return false;
        }
      }

      return true;
    }

    return false;
  }

  protected void setChunkLength(byte[] hexLengthBytes) {
    chunkLength = Long.parseLong(new String(hexLengthBytes, UTF_8).trim(), 16);
  }

  protected void extractAlgorithmAndChecksum() throws IOException {
    if (algorithm == null && checksum == null) {
      readUntil(CHECKSUM_HEADER);
      var typeAndChecksum = readUntil(CRLF);
      var typeAndChecksumString = new String(typeAndChecksum);
      if (!typeAndChecksumString.isBlank()) {
        var split = typeAndChecksumString.split(":");
        var type = split[0];
        algorithm = ChecksumAlgorithm.fromString(type);
        checksum = split[1];
      }
    }
  }

  public String getChecksum() {
    return checksum;
  }

  public ChecksumAlgorithm getAlgorithm() {
    return algorithm;
  }
}
