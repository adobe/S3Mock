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

import com.adobe.testing.s3mock.ChecksumTestUtil
import com.adobe.testing.s3mock.ChecksumTestUtil.prepareInputStream
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import java.io.File
import java.util.stream.Stream
import software.amazon.awssdk.checksums.spi.ChecksumAlgorithm as AwsChecksumAlgorithm

internal class AwsUnsignedChunkedDecodingChecksumInputStreamTest {
  @Test
  fun `test decoding aws inputstream with fixed checksum`(testInfo: TestInfo) {
    val sampleFile = TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt")
    val sampleFileLarge = TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt")
    doTest(
      sampleFile,
      1,
      DefaultChecksumAlgorithm.SHA256,
      "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE=",
      ChecksumAlgorithm.SHA256,
    )
    doTest(
      sampleFileLarge,
      16,
      DefaultChecksumAlgorithm.SHA256,
      "Y8S4/uAGut7vjdFZQjLKZ7P28V9EPWb4BIoeniuM0mY=",
      ChecksumAlgorithm.SHA256
    )
  }

  @ParameterizedTest
  @MethodSource("algorithms")
  fun `test decoding aws inputstream with calculated checksum`(
    algorithm: AwsChecksumAlgorithm,
    testInfo: TestInfo
  ) {
    val checksumAlgorithm = ChecksumAlgorithm.fromString(algorithm.toString())
    val sampleFile = TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt")
    val sampleFileLarge = TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt")
    val testImageSmall = TestUtil.getFileFromClasspath(testInfo, "test-image-small.png")
    val testImage = TestUtil.getFileFromClasspath(testInfo, "test-image.png")

    doTest(
      sampleFile,
      1,
      algorithm,
      DigestUtil.checksumFor(sampleFile.toPath(), algorithm),
      checksumAlgorithm
    )
    doTest(
      sampleFileLarge,
      16,
      algorithm,
      DigestUtil.checksumFor(sampleFileLarge.toPath(), algorithm),
      checksumAlgorithm
    )
    doTest(
      testImageSmall,
      9,
      algorithm,
      DigestUtil.checksumFor(testImageSmall.toPath(), algorithm),
      checksumAlgorithm
    )
    doTest(
      testImage,
      17,
      algorithm,
      DigestUtil.checksumFor(testImage.toPath(), algorithm),
      checksumAlgorithm
    )
  }

  @Test
  fun `test decoding aws inputstream without checksum`(testInfo: TestInfo) {
    val sampleFile = TestUtil.getFileFromClasspath(testInfo, "sampleFile.txt")
    val sampleFileLarge = TestUtil.getFileFromClasspath(testInfo, "sampleFile_large.txt")
    val testImageSmall = TestUtil.getFileFromClasspath(testInfo, "test-image-small.png")
    val testImage = TestUtil.getFileFromClasspath(testInfo, "test-image.png")
    doTest(sampleFile, 1)
    doTest(sampleFileLarge, 16)
    doTest(testImageSmall, 9)
    doTest(testImage, 17)
  }

  private fun doTest(
    input: File,
    chunks: Int = 0,
    algorithm: AwsChecksumAlgorithm? = null,
    checksum: String? = null,
    checksumAlgorithm: ChecksumAlgorithm? = null
  ) {
    val (chunkedEncodingInputStream, decodedLength) = prepareInputStream(
      input,
      false,
      algorithm,
    )
    val iut = AwsUnsignedChunkedDecodingChecksumInputStream(chunkedEncodingInputStream, decodedLength)
    assertThat(iut).hasSameContentAs(input.inputStream())
    assertThat(iut.algorithm).isEqualTo(checksumAlgorithm)
    assertThat(iut.checksum).isEqualTo(checksum)
    assertThat(iut.decodedLength).isEqualTo(decodedLength)
    assertThat(iut.readDecodedLength).isEqualTo(decodedLength)
    assertThat(iut.chunks).isEqualTo(chunks)
  }

  companion object {
    @JvmStatic
    fun algorithms(): Stream<AwsChecksumAlgorithm> = ChecksumTestUtil.algorithms()
  }
}
