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

import java.io.IOException;
import java.io.InputStream;

/**
 * Skips V4 style signing metadata from input streams.
 * <p>The original stream looks like this (newlines are CRLF):</p>
 *
 * <pre>
 * 5;chunk-signature=7ece820edcf094ce1ef6d643c8db60b67913e28831d9b0430efd2b56a9deec5e
 * 12345
 * 0;chunk-signature=ee2c094d7162170fcac17d2c76073cd834b0488bfe52e89e48599b8115c7ffa2
 * </pre>
 *
 * <p>The format of each chunk of data is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][crlf]
 * [payload-bytes-of-this-chunk][crlf]
 * </pre>
 *
 * @see
 * <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AwsChunkedEncodingInputStream.html">
 *     AwsChunkedEncodingInputStream</a>
 */
public class AwsChunkedDecodingInputStream extends AbstractAwsInputStream {

  /**
   * Constructs a new {@link AwsChunkedDecodingInputStream}.
   *
   * @param source The {@link InputStream} to wrap.
   */
  public AwsChunkedDecodingInputStream(InputStream source) {
    super(source);
  }

  @Override
  public int read() throws IOException {
    if (payloadLength == 0L) {
      var hexLengthBytes = readUntil(DELIMITER);
      if (hexLengthBytes.length == 0) {
        return -1;
      }

      setPayloadLength(hexLengthBytes);

      if (payloadLength == 0L) {
        return -1;
      }

      readUntil(CRLF);
    }

    payloadLength--;

    return source.read();
  }
}
