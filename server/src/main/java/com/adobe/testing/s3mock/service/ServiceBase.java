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

package com.adobe.testing.s3mock.service;

import static com.adobe.testing.s3mock.S3Exception.BAD_CHECKSUM_CRC32;
import static com.adobe.testing.s3mock.S3Exception.BAD_CHECKSUM_CRC32C;
import static com.adobe.testing.s3mock.S3Exception.BAD_CHECKSUM_SHA1;
import static com.adobe.testing.s3mock.S3Exception.BAD_CHECKSUM_SHA256;
import static com.adobe.testing.s3mock.S3Exception.BAD_DIGEST;
import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_CONTENT;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DECODED_CONTENT_LENGTH;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk;
import static com.adobe.testing.s3mock.util.HeaderUtil.isChunked;
import static com.adobe.testing.s3mock.util.HeaderUtil.isChunkedAndV4Signed;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.util.AbstractAwsInputStream;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingChecksumInputStream;
import com.adobe.testing.s3mock.util.AwsUnsignedChunkedDecodingChecksumInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.http.HttpHeaders;

abstract class ServiceBase {

  public void verifyChecksum(Path path, String checksum, ChecksumAlgorithm checksumAlgorithm) {
    String checksumFor = DigestUtil.checksumFor(path, checksumAlgorithm.toAlgorithm());
    if (!checksum.equals(checksumFor)) {
      switch (checksumAlgorithm) {
        case SHA1 -> throw BAD_CHECKSUM_SHA1;
        case SHA256 -> throw BAD_CHECKSUM_SHA256;
        case CRC32 -> throw BAD_CHECKSUM_CRC32;
        case CRC32C -> throw BAD_CHECKSUM_CRC32C;
        default -> throw BAD_DIGEST;
      }
    }
  }

  public Pair<Path, String> toTempFile(InputStream inputStream, HttpHeaders httpHeaders) {
    try {
      var tempFile = Files.createTempFile("ObjectService", "toTempFile");
      try (var os = Files.newOutputStream(tempFile);
           var wrappedStream = wrapStream(inputStream, httpHeaders)) {
        wrappedStream.transferTo(os);
        ChecksumAlgorithm algorithmFromSdk = checksumAlgorithmFromSdk(httpHeaders);
        if (algorithmFromSdk != null
            && wrappedStream instanceof AbstractAwsInputStream awsInputStream) {
          return Pair.of(tempFile, awsInputStream.getChecksum());
        }
        return Pair.of(tempFile, null);
      }
    } catch (IOException e) {
      throw BAD_REQUEST_CONTENT;
    }
  }

  private InputStream wrapStream(InputStream dataStream, HttpHeaders headers) {
    var lengthHeader = headers.getFirst(X_AMZ_DECODED_CONTENT_LENGTH);
    var length = lengthHeader == null ? -1 : Long.parseLong(lengthHeader);
    if (isChunkedAndV4Signed(headers)) {
      return new AwsChunkedDecodingChecksumInputStream(dataStream, length);
    } else if (isChunked(headers)) {
      return new AwsUnsignedChunkedDecodingChecksumInputStream(dataStream, length);
    } else {
      return dataStream;
    }
  }
}
