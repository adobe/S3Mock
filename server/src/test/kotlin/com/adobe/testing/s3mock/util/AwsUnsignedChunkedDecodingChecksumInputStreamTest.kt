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
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.checksums.SdkChecksum
import software.amazon.awssdk.core.internal.chunked.AwsChunkedEncodingConfig
import software.amazon.awssdk.core.internal.io.AwsUnsignedChunkedEncodingInputStream
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.util.Arrays.stream
import java.util.stream.Stream

internal class AwsUnsignedChunkedDecodingChecksumInputStreamTest {
  @Test
  @Throws(IOException::class)
  fun testDecode_checksum(testInfo: TestInfo) {
    doTest(
      testInfo, "sampleFile.txt", AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
      "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=", ChecksumAlgorithm.SHA256
    )
    doTest(
      testInfo, "sampleFile_large.txt", AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256, Algorithm.SHA256,
      "Y8S4/uAGut7vjdFZQjLKZ7P28V9EPWb4BIoeniuM0mY=", ChecksumAlgorithm.SHA256
    )
  }

  @Test
  @Throws(IOException::class)
  fun testDecode_noChecksum(testInfo: TestInfo) {
    doTest(testInfo, "sampleFile.txt")
    doTest(testInfo, "sampleFile_large.txt")
  }

  @JvmOverloads
  @Throws(IOException::class)
  fun doTest(
    testInfo: TestInfo, fileName: String?, header: String? = null, algorithm: Algorithm? = null,
    checksum: String? = null, checksumAlgorithm: ChecksumAlgorithm? = null
  ) {
    val sampleFile = TestUtil.getFileFromClasspath(testInfo, fileName)
    val builder = AwsUnsignedChunkedEncodingInputStream
      .builder()
      .inputStream(Files.newInputStream(sampleFile.toPath()))
    if (algorithm != null) {
      builder.sdkChecksum(SdkChecksum.forAlgorithm(algorithm))
    }
    val chunkedEncodingInputStream: InputStream = builder
      .checksumHeaderForTrailer(header) //force chunks in the inputstream
      .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
      .build()

    val decodedLength = sampleFile.length()
    val iut = AwsUnsignedChunkedDecodingChecksumInputStream(chunkedEncodingInputStream, decodedLength)

    assertThat(iut).hasSameContentAs(Files.newInputStream(sampleFile.toPath()))
    assertThat(iut.getAlgorithm()).isEqualTo(checksumAlgorithm)
    assertThat(iut.getChecksum()).isEqualTo(checksum)
    assertThat(iut.decodedLength).isEqualTo(decodedLength)
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength)
  }

  @ParameterizedTest
  @MethodSource("algorithms")
  @Throws(IOException::class)
  fun testDecode_unsigned_checksum(algorithm: Algorithm, testInfo: TestInfo) {
    val checksumAlgorithm = ChecksumAlgorithm.fromString(algorithm.toString())
    val header = HeaderUtil.mapChecksumToHeader(checksumAlgorithm)
    doTestUnsigned(
      TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt"),
      1,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestUnsigned(
      TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt"),
      16,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestUnsigned(
      TestUtil.getFileFromClasspath(testInfo, "test-image-small.png"),
      9,
      header,
      SdkChecksum.forAlgorithm(algorithm),
      DigestUtil.checksumFor(
        TestUtil.getFileFromClasspath(testInfo, "test-image-small.png").toPath(), algorithm
      ),
      checksumAlgorithm
    )
    doTestUnsigned(
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
  fun testDecode_unsigned_noChecksum(testInfo: TestInfo) {
    doTestUnsigned(TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt"), 1)
    doTestUnsigned(TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt"), 16)
    doTestUnsigned(TestUtil.getFileFromClasspath(testInfo, "test-image-small.png"), 9)
    doTestUnsigned(TestUtil.getFileFromClasspath(testInfo, "test-image.png"), 17)
  }

  @JvmOverloads
  @Throws(IOException::class)
  fun doTestUnsigned(
    input: File, chunks: Int, header: String? = null, algorithm: SdkChecksum? = null,
    checksum: String? = null, checksumAlgorithm: ChecksumAlgorithm? = null
  ) {
    val chunkedEncodingInputStream: InputStream = AwsUnsignedChunkedEncodingInputStream
      .builder()
      .inputStream(Files.newInputStream(input.toPath()))
      .sdkChecksum(algorithm)
      .checksumHeaderForTrailer(header) //force chunks in the inputstream
      .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
      .build()

    val decodedLength = input.length()
    val iut =
      AwsUnsignedChunkedDecodingChecksumInputStream(
        chunkedEncodingInputStream,
        decodedLength
      )

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
      return stream(Algorithm.entries.toTypedArray()).filter { it != Algorithm.CRC64NVME }
    }
  }
}
