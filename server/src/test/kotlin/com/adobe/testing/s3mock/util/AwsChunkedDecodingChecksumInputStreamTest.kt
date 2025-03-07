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
package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsSignedChunkedEncodingInputStream
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.checksums.SdkChecksum
import software.amazon.awssdk.core.internal.chunked.AwsChunkedEncodingConfig
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.util.stream.Stream

internal class AwsChunkedDecodingChecksumInputStreamTest {
  @Test
  @Throws(IOException::class)
  fun testDecode_checksum(testInfo: TestInfo) {
    doTest(
      testInfo, "sampleFile.txt", AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
      "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=", ChecksumAlgorithm.SHA256, 1
    )
    doTest(
      testInfo, "sampleFile_large.txt", AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
      "Y8S4/uAGut7vjdFZQjLKZ7P28V9EPWb4BIoeniuM0mY=", ChecksumAlgorithm.SHA256, 16
    )
  }

  @Test
  @Throws(IOException::class)
  fun testDecode_noChecksum(testInfo: TestInfo) {
    doTest(testInfo, "sampleFile.txt", 1)
    doTest(testInfo, "sampleFile_large.txt", 16)
  }

  @Throws(IOException::class)
  fun doTest(testInfo: TestInfo, fileName: String, chunks: Int) {
    doTest(testInfo, fileName, null, null, null, null, chunks)
  }

  @Throws(IOException::class)
  fun doTest(
    testInfo: TestInfo, fileName: String, header: String?, algorithm: Algorithm?,
    checksum: String?, checksumAlgorithm: ChecksumAlgorithm?, chunks: Int
  ) {
    val sampleFile = TestUtil.getFileFromClasspath(testInfo, fileName)
    val builder = AwsSignedChunkedEncodingInputStream
      .builder()
      .inputStream(Files.newInputStream(sampleFile.toPath()))
    if (algorithm != null) {
      builder.sdkChecksum(SdkChecksum.forAlgorithm(algorithm))
    }
    val chunkedEncodingInputStream: InputStream = builder
      .checksumHeaderForTrailer(header) //force chunks in the inputstream
      .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
      .awsChunkSigner(
        AwsS3V4ChunkSigner(
          "signingKey".toByteArray(),
          "dateTime",
          "keyPath"
        )
      )
      .build()

    val decodedLength = sampleFile.length()
    val iut = AwsChunkedDecodingChecksumInputStream(chunkedEncodingInputStream, decodedLength)
    assertThat(iut).hasSameContentAs(Files.newInputStream(sampleFile.toPath()))
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm)
    assertThat(iut.getChecksum()).isEqualTo(checksum)
    assertThat(iut.decodedLength).isEqualTo(decodedLength)
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength)
    assertThat(iut.chunks).isEqualTo(chunks)
  }

  @ParameterizedTest
  @MethodSource("algorithms")
  @Throws(IOException::class)
  fun testDecode_signed_checksum(algorithm: Algorithm, testInfo: TestInfo) {
    val checksumAlgorithm = ChecksumAlgorithm.fromString(algorithm.toString())
    val header = HeaderUtil.mapChecksumToHeader(checksumAlgorithm)
    doTestSigned(
      TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt"),
      1,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestSigned(
      TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt"),
      16,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestSigned(
      TestUtil.getFileFromClasspath(testInfo, "test-image-small.png"),
      9,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "test-image-small.png").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestSigned(
      TestUtil.getFileFromClasspath(testInfo, "test-image.png"),
      17,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "test-image.png").toPath(), algorithm
      ),
      checksumAlgorithm
    )
  }

  @Test
  @Throws(IOException::class)
  fun testDecode_signed_noChecksum(testInfo: TestInfo) {
    doTestSigned(TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt"), 1)
    doTestSigned(TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt"), 16)
    doTestSigned(TestUtil.getFileFromClasspath(testInfo, "test-image-small.png"), 9)
    doTestSigned(TestUtil.getFileFromClasspath(testInfo, "test-image.png"), 17)
  }

  @JvmOverloads
  @Throws(IOException::class)
  fun doTestSigned(
    input: File, chunks: Int, header: String? = null, algorithm: SdkChecksum? = null,
    checksum: String? = null, checksumAlgorithm: ChecksumAlgorithm? = null
  ) {
    val chunkedEncodingInputStream: InputStream = AwsSignedChunkedEncodingInputStream
      .builder()
      .inputStream(Files.newInputStream(input.toPath()))
      .sdkChecksum(algorithm)
      .checksumHeaderForTrailer(header) //force chunks in the inputstream
      .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
      .awsChunkSigner(
        AwsS3V4ChunkSigner(
          "signingKey".toByteArray(),
          "dateTime",
          "keyPath"
        )
      )
      .build()

    val decodedLength = input.length()
    val iut = AwsChunkedDecodingChecksumInputStream(chunkedEncodingInputStream, decodedLength)

    assertThat(iut).hasSameContentAs(Files.newInputStream(input.toPath()))
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm)
    assertThat(iut.getChecksum()).isEqualTo(checksum)
    assertThat(iut.decodedLength).isEqualTo(decodedLength)
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength)
    assertThat(iut.chunks).isEqualTo(chunks)
  }

  companion object {
    @JvmStatic
    private fun algorithms(): Stream<Algorithm> {
      //ignore new CRC64NVME for now, looks like AWS is moving checksums from core to a new checksums module, but not all
      //types are currently compatible with the new types.
      return Algorithm.entries.toList().stream().filter { it != Algorithm.CRC64NVME }
    }
  }
}
