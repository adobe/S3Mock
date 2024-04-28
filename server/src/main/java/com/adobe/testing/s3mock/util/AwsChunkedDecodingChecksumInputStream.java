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

import java.io.IOException;
import java.io.InputStream;

/**
 * Merges chunks from AWS chunked AwsChunkedEncodingInputStream, skipping V4 style signing metadata.
 * <p>The original stream looks like this (newlines are CRLF):</p>
 *
 * <pre>
 * 24;chunk-signature=312a41de690364ad6d17629d1e026c448e78abd328f1602276fdd2c3f928d100
 * ## sample test file ##
 *
 * demo=content
 * 0;chunk-signature=4d2b448448f29b473beb81340f5a3d6c9468e4fbb9ac761cfab63846919011fb
 * </pre>
 *
 * <p>The format of each chunk of data is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][crlf]
 * [payload-bytes-of-this-chunk][crlf]
 * 0;chunk-signature=[sha256-signature][crlf]
 * x-amz-checksum-[checksum-algoritm]:[checksum]
 * </pre>
 *
 * @see
 * <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/internal/io/AwsChunkedEncodingInputStream.html">
 *     AwsChunkedEncodingInputStream</a>
 */
public class AwsChunkedDecodingChecksumInputStream extends AbstractAwsInputStream {

  public AwsChunkedDecodingChecksumInputStream(InputStream source, long decodedLength) {
    super(source, decodedLength);
  }

  @Override
  public int read() throws IOException {
    if (chunkLength == 0L) {
      //try to read chunk length
      var hexLengthBytes = readUntil(DELIMITER);
      if (hexLengthBytes.length == 0) {
        return -1;
      }

      setChunkLength(hexLengthBytes);

      if (chunkLength == 0L) {
        extractAlgorithmAndChecksum();
        return -1;
      }

      readUntil(CRLF);
    }

    readDecodedLength++;
    chunkLength--;

    return source.read();
  }
}
