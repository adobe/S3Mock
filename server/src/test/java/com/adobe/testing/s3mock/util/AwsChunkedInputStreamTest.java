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

import static com.adobe.testing.s3mock.util.DigestUtil.checksumFor;
import static com.adobe.testing.s3mock.util.TestUtil.getFileFromClasspath;
import static java.nio.file.Files.newInputStream;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsSignedChunkedEncodingInputStream;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.internal.chunked.AwsChunkedEncodingConfig;
import software.amazon.awssdk.core.internal.io.AwsUnsignedChunkedEncodingInputStream;

class AwsChunkedInputStreamTest {

  @ParameterizedTest
  @MethodSource("algorithms")
  void testDecode_unsigned_checksum(Algorithm algorithm, TestInfo testInfo) throws IOException {
    ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.fromString(algorithm.toString());
    String header = HeaderUtil.mapChecksumToHeader(checksumAlgorithm);
    doTestUnsigned(getFileFromClasspath(testInfo, "sampleFile.txt"),
        1,
        header,
        SdkChecksum.forAlgorithm(algorithm),
        checksumFor(
            getFileFromClasspath(testInfo, "sampleFile.txt").toPath(), algorithm),
        checksumAlgorithm
    );
    doTestUnsigned(getFileFromClasspath(testInfo, "sampleFile_large.txt"),
        16,
        header,
        SdkChecksum.forAlgorithm(algorithm),
        checksumFor(
            getFileFromClasspath(testInfo, "sampleFile_large.txt").toPath(), algorithm),
        checksumAlgorithm
    );
  }

  @Test
  void testDecode_unsigned_noChecksum(TestInfo testInfo) throws IOException {
    doTestUnsigned(getFileFromClasspath(testInfo, "sampleFile.txt"), 1);
    doTestUnsigned(getFileFromClasspath(testInfo, "sampleFile_large.txt"), 16);
  }

  void doTestUnsigned(File input, int chunks) throws IOException {
    doTestUnsigned(input, chunks, null, null, null, null);
  }

  void doTestUnsigned(File input, int chunks, String header, SdkChecksum algorithm,
                      String checksum, ChecksumAlgorithm checksumAlgorithm) throws IOException {
    InputStream chunkedEncodingInputStream = AwsUnsignedChunkedEncodingInputStream
        .builder()
        .inputStream(newInputStream(input.toPath()))
        .sdkChecksum(algorithm)
        .checksumHeaderForTrailer(header)
        //force chunks in the inputstream
        .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
        .build();

    long decodedLength = input.length();
    AwsChunkedInputStream iut = new AwsChunkedInputStream(chunkedEncodingInputStream,
        decodedLength,
        checksumAlgorithm != null);

    assertThat(iut).hasSameContentAs(newInputStream(input.toPath()));
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm);
    assertThat(iut.getChecksum()).isEqualTo(checksum);
    assertThat(iut.decodedLength).isEqualTo(decodedLength);
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength);
    assertThat(iut.chunks).isEqualTo(chunks);
  }

  @ParameterizedTest
  @MethodSource("algorithms")
  void testDecode_signed_checksum(Algorithm algorithm, TestInfo testInfo) throws IOException {
    ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.fromString(algorithm.toString());
    String header = HeaderUtil.mapChecksumToHeader(checksumAlgorithm);
    doTestSigned(getFileFromClasspath(testInfo, "sampleFile.txt"),
        1,
        header,
        SdkChecksum.forAlgorithm(algorithm),
        checksumFor(
            getFileFromClasspath(testInfo, "sampleFile.txt").toPath(), algorithm),
        checksumAlgorithm
    );
    doTestSigned(getFileFromClasspath(testInfo, "sampleFile_large.txt"),
        16,
        header,
        SdkChecksum.forAlgorithm(algorithm),
        checksumFor(
            getFileFromClasspath(testInfo, "sampleFile_large.txt").toPath(), algorithm),
        checksumAlgorithm
    );
  }

  @Test
  void testDecode_signed_noChecksum(TestInfo testInfo) throws IOException {
    doTestSigned(getFileFromClasspath(testInfo, "sampleFile.txt"), 1);
    doTestSigned(getFileFromClasspath(testInfo, "sampleFile_large.txt"), 16);
  }

  void doTestSigned(File input, int chunks) throws IOException {
    doTestSigned(input, chunks, null, null, null, null);
  }

  void doTestSigned(File input, int chunks, String header, SdkChecksum algorithm,
                    String checksum, ChecksumAlgorithm checksumAlgorithm) throws IOException {
    InputStream chunkedEncodingInputStream = AwsSignedChunkedEncodingInputStream
        .builder()
        .inputStream(newInputStream(input.toPath()))
        .sdkChecksum(algorithm)
        .checksumHeaderForTrailer(header)
        //force chunks in the inputstream
        .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
        .awsChunkSigner(new AwsS3V4ChunkSigner("signingKey".getBytes(),
            "dateTime",
            "keyPath"))
        .build();

    long decodedLength = input.length();
    AwsChunkedInputStream iut = new AwsChunkedInputStream(chunkedEncodingInputStream, decodedLength,
        checksumAlgorithm != null);

    assertThat(iut).hasSameContentAs(newInputStream(input.toPath()));
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm);
    assertThat(iut.getChecksum()).isEqualTo(checksum);
    assertThat(iut.decodedLength).isEqualTo(decodedLength);
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength);
    assertThat(iut.chunks).isEqualTo(chunks);
  }

  private static Stream<Algorithm> algorithms() {
    return Arrays.stream(Algorithm.values());
  }
}
