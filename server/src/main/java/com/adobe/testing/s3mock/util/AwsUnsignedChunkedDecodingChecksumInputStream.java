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

package com.adobe.testing.s3mock.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Merges chunks from AWS chunked AwsUnsignedChunkedEncodingInputStream.
 * The checksum is optionally included in the stream as part of the "trail headers"
 * after the last chunk.
 *
 * <p>The original stream looks like this:</p>
 *
 * <pre>
 * 24
 * ## sample test file ##
 *
 * demo=content
 * 0
 * x-amz-checksum-sha256:1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=
 * </pre>
 *
 * <p>The format of each chunk of data is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * </pre>
 *
 * <p>The format of the full payload is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * 0[EOL]
 * x-amz-checksum-[checksum-algoritm]:[checksum][EOL]
 * [other trail headers]
 * </pre>
 *
 * @see
 * <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/internal/io/AwsUnsignedChunkedEncodingInputStream.html">
 *     AwsUnsignedChunkedEncodingInputStream</a>
 */
public class AwsUnsignedChunkedDecodingChecksumInputStream extends AbstractAwsInputStream {

  public AwsUnsignedChunkedDecodingChecksumInputStream(InputStream source, long decodedLength) {
    super(source, decodedLength);
  }

  @Override
  public int read() throws IOException {
    if (chunkLength == 0L) {
      // try to read chunk length
      var hexLengthBytes = readHexlength();
      if (hexLengthBytes.length == 0) {
        return -1;
      }

      setChunkLength(hexLengthBytes);

      if (chunkLength == 0L) {
        // chunk length found, but was "0". Try and find the checksum.
        extractAlgorithmAndChecksum();
        return -1;
      }

      chunks++;
    }

    readDecodedLength++;
    chunkLength--;

    return source.read();
  }

  private byte[] readHexlength() throws IOException {
    var hexLengthBytes = readUntil(CRLF);
    if (hexLengthBytes.length == 0) {
      hexLengthBytes = readUntil(CRLF);
    }
    return hexLengthBytes;
  }
}
