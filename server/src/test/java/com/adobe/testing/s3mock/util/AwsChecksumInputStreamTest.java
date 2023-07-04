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

import static com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32;
import static com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32C;
import static com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA1;
import static com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32C;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA1;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256;
import static com.adobe.testing.s3mock.util.TestUtil.getFileFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.internal.io.AwsUnsignedChunkedEncodingInputStream;

class AwsChecksumInputStreamTest {

  @Test
  void testSha1(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt", X_AMZ_CHECKSUM_SHA1, Algorithm.SHA1,
        "+AXXQmKfnxMv0B57SJutbNpZBww=", SHA1);
    doTest(testInfo, "sampleFile_large.txt", X_AMZ_CHECKSUM_SHA1, Algorithm.SHA1,
        "mi4mRhhRLocVrmKp/H4qOEltQy4=", SHA1);
  }

  @Test
  void testCrc32(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt", X_AMZ_CHECKSUM_CRC32, Algorithm.CRC32,
        "I6zdvg==", CRC32);
    doTest(testInfo, "sampleFile_large.txt", X_AMZ_CHECKSUM_CRC32, Algorithm.CRC32,
        "xXk0iw==", CRC32);
  }

  @Test
  void testCrc32c(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt", X_AMZ_CHECKSUM_CRC32C, Algorithm.CRC32C,
        "/Ho1Kg==", CRC32C);
    doTest(testInfo, "sampleFile_large.txt", X_AMZ_CHECKSUM_CRC32C, Algorithm.CRC32C,
        "SOMAUA==", CRC32C);
  }

  @Test
  void testSha256(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt", X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
        "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=", SHA256);
    doTest(testInfo, "sampleFile_large.txt", X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
        "BNNY15scjxN9jPa+AEJ6p08Cfm4BhYOXkhladr4QSTs=", SHA256);
  }

  void doTest(TestInfo testInfo, String fileName, String header, Algorithm algorithm,
              String checksum, ChecksumAlgorithm checksumAlgorithm) throws IOException {
    File sampleFile = getFileFromClasspath(testInfo, fileName);
    InputStream chunkedEncodingInputStream = AwsUnsignedChunkedEncodingInputStream
        .builder()
        .inputStream(Files.newInputStream(sampleFile.toPath()))
        .sdkChecksum(SdkChecksum.forAlgorithm(algorithm))
        .checksumHeaderForTrailer(header)
        .build();
    AwsChecksumInputStream iut = new AwsChecksumInputStream(chunkedEncodingInputStream);
    assertThat(iut).hasSameContentAs(Files.newInputStream(sampleFile.toPath()));
    assertThat(iut.getChecksum()).isEqualTo(checksum);
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm);
  }
}
