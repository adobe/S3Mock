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

import static com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256;
import static com.adobe.testing.s3mock.util.TestUtil.getFileFromClasspath;
import static com.adobe.testing.s3mock.util.TestUtil.getPayloadFile;
import static com.adobe.testing.s3mock.util.TestUtil.getTestFile;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsSignedChunkedEncodingInputStream;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.core.checksums.SdkChecksum;

class AwsChunkedDecodingChecksumInputStreamTest {

  @Test
  void testDecode(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt", X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
        "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=", SHA256);
    doTest(testInfo, "sampleFile_large.txt", X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
        "BNNY15scjxN9jPa+AEJ6p08Cfm4BhYOXkhladr4QSTs=", SHA256);
  }

  void doTest(TestInfo testInfo, String fileName, String header, Algorithm algorithm,
              String checksum, ChecksumAlgorithm checksumAlgorithm) throws IOException {
    File sampleFile = getFileFromClasspath(testInfo, fileName);
    InputStream chunkedEncodingInputStream = AwsSignedChunkedEncodingInputStream
        .builder()
        .inputStream(Files.newInputStream(sampleFile.toPath()))
        .sdkChecksum(SdkChecksum.forAlgorithm(algorithm))
        .checksumHeaderForTrailer(header)
        .awsChunkSigner(new AwsS3V4ChunkSigner("signingKey".getBytes(),
            "dateTime",
            "keyPath"))
        .build();
    AwsChunkedDecodingChecksumInputStream iut = new
        AwsChunkedDecodingChecksumInputStream(chunkedEncodingInputStream);
    assertThat(iut).hasSameContentAs(Files.newInputStream(sampleFile.toPath()));
    assertThat(iut.getChecksum()).isEqualTo(checksum);
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm);
  }
}
