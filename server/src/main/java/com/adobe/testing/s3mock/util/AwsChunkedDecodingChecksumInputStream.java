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

public class AwsChunkedDecodingChecksumInputStream extends AwsChecksumInputStream {

  public AwsChunkedDecodingChecksumInputStream(InputStream source) {
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
        extractAlgorithmAndChecksum();
        return -1;
      }

      readUntil(CRLF);
    }

    payloadLength--;

    return source.read();
  }
}
