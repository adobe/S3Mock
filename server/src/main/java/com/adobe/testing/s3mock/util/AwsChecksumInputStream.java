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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads checksum from incoming stream.
 * When the AWS client sends a checksum in the request, it's embedded into the request body and
 * surrounds the payload.
 * The stream looks like this:
 * <pre>
 * 24
 * ## sample test file ##
 *
 * demo=content
 * 0
 * x-amz-checksum-sha1:+AXXQmKfnxMv0B57SJutbNpZBww=
 * </pre>
 * The format is this:
 * <pre>
 * [hex-encoded-number-of-bytes-in-payload][crlf]
 * [payload-bytes]
 * 0[crlf]
 * [amazon-checksum-header]:[checksum][crlf]
 * </pre>
 */
public class AwsChecksumInputStream extends AbstractAwsInputStream {
  protected static final byte[] CHECKSUM_HEADER =
      "x-amz-checksum-".getBytes(StandardCharsets.UTF_8);
  protected String checksum;
  protected ChecksumAlgorithm algorithm;

  public AwsChecksumInputStream(InputStream source) {
    super(source);
  }

  @Override
  public int read() throws IOException {
    //read payload length from first line
    if (payloadLength == 0L) {
      byte[] hexLengthBytes = readUntil(CRLF);
      if (hexLengthBytes.length == 0) {
        return -1;
      }
      setPayloadLength(hexLengthBytes);
    }

    //read all bytes of the payload
    if (payloadLength > 0L) {
      payloadLength--;
      return source.read();
    }

    //read the last lines which contain the algorithm and the checksum
    extractAlgorithmAndChecksum();

    return -1;
  }

  protected void extractAlgorithmAndChecksum() throws IOException {
    if (algorithm == null && checksum == null) {
      readUntil(CHECKSUM_HEADER);
      byte[] typeAndChecksum = readUntil(CRLF);
      String typeAndChecksumString = new String(typeAndChecksum);
      String[] split = typeAndChecksumString.split(":");
      String type = split[0];
      algorithm = ChecksumAlgorithm.fromString(type);
      checksum = split[1];
    }
  }

  public String getChecksum() {
    return checksum;
  }

  public ChecksumAlgorithm getAlgorithm() {
    return algorithm;
  }
}
