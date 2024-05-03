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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * This handles both signed and unsigned chunked encodings.
 *
 * <p>SIGNED:</p>
 * Merges chunks from AWS chunked AwsChunkedEncodingInputStream, skipping V4 style signing metadata.
 * The checksum is optionally included in the stream as part of the "trail headers"
 * after the last chunk.
 *
 * <p>The original stream looks like this:</p>
 *
 * <pre>
 * 24;chunk-signature=312a41de690364ad6d17629d1e026c448e78abd328f1602276fdd2c3f928d100
 * ## sample test file ##
 *
 * demo=content
 * 0;chunk-signature=4d2b448448f29b473beb81340f5a3d6c9468e4fbb9ac761cfab63846919011fb
 * x-amz-checksum-sha256:1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=
 * </pre>
 *
 * <p>The format of each chunk of data is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * </pre>
 *
 * <p>The format of the full payload is:</p>
 *
 * <pre>
 * [hex-encoded-number-of-bytes-in-chunk];chunk-signature=[sha256-signature][EOL]
 * [payload-bytes-of-this-chunk][EOL]
 * 0;chunk-signature=[sha256-signature][EOL]
 * x-amz-checksum-[checksum-algorithm]:[checksum][EOL]
 * [other trail headers]
 * </pre>
 *
 * <p>UNSIGNED:</p>
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
 *
 * @see
 * <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/internal/io/AwsChunkedEncodingInputStream.html">
 *     AwsChunkedEncodingInputStream</a>
 */
public class AwsChunkedInputStream extends InputStream {
  protected long readDecodedLength = 0L;
  protected BufferedInputStream source;
  protected BufferedReader reader;
  protected long chunkLength = 0L;
  protected int chunks = 0;
  protected String checksum;
  protected ChecksumAlgorithm algorithm;
  protected final boolean hasChecksum;
  protected final long decodedLength;

  public AwsChunkedInputStream(InputStream source, long decodedLength, boolean hasChecksum) {
    this.reader = new BufferedReader(new InputStreamReader(source, UTF_8));
    this.decodedLength = decodedLength;
    this.hasChecksum = hasChecksum;
  }

  @Override
  public int read() throws IOException {
    if (chunkLength == -1L) {
      //stream end was marked after last chunk was read.
      // See below.
      // Always return -1.
      return -1;
    }
    if (chunkLength == 0L) {
      //try to read chunk length
      var hexLength = readHexLength();
      if (hexLength == null) {
        return -1;
      }
      setChunkLength(hexLength);

      if (chunkLength == 0L) {
        //chunk length found, but was "0".
        // This marks the end of the payload and the beginning of the trail headers.
        // Extract checksum, if available.
        extractAlgorithmAndChecksum();
        // make sure that no more bytes are read from the stream, see first line.
        chunkLength = -1L;
        return -1;
      }
      chunks++;
    }

    readDecodedLength++;
    chunkLength--;

    return reader.read();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public String getChecksum() {
    return checksum;
  }

  public ChecksumAlgorithm getAlgorithm() {
    return algorithm;
  }

  private void setChunkLength(String hexLength) {
    chunkLength = Long.parseLong(hexLength.trim(), 16);
  }

  private void extractAlgorithmAndChecksum() throws IOException {
    if (hasChecksum) {
      var typeAndChecksumString = reader.readLine();
      if (typeAndChecksumString != null && !typeAndChecksumString.isBlank()) {
        var split = typeAndChecksumString.split(":");
        var type = split[0];
        algorithm = ChecksumAlgorithm.fromHeader(type);
        checksum = split[1];
      }
    }
  }

  private String readHexLength() throws IOException {
    var hexLength = reader.readLine();
    if (hexLength != null && hexLength.isBlank()) {
      //skip empty line if present
      hexLength = reader.readLine();
    }
    if (hexLength != null && !hexLength.isBlank()) {
      //remove chunk signature, if present
      hexLength = hexLength.split(";")[0];
    }
    return hexLength;
  }
}
