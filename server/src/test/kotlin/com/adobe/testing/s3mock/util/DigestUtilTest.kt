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
package com.adobe.testing.s3mock.util

import org.apache.commons.codec.digest.DigestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.utils.BinaryUtils

internal class DigestUtilTest {
  @Test
  fun testHexDigestOfMultipleFiles(testInfo: TestInfo) {
    //yes, this is correct - AWS calculates a Multipart digest by calculating the digest of every
    //file involved, and then calculates the digest on the result.
    //a hyphen with the part count is added as a suffix.
    val expected = "${
      DigestUtils.md5Hex(
        DigestUtils.md5("Part1")  //testFile1
          + DigestUtils.md5("Part2") //testFile2
      )
    }-2"

    //files contain the exact content seen above
    val files = listOf(
      TestUtil.getTestFile(testInfo, "testFile1").toPath(),
      TestUtil.getTestFile(testInfo, "testFile2").toPath()
    )

    assertThat(DigestUtil.hexDigestMultipart(files)).isEqualTo(expected)
  }

  @Test
  fun testChecksumOfMultipleFiles(testInfo: TestInfo) {
    //yes, this is correct - AWS calculates a Multipart digest by calculating the digest of every
    //file involved, and then calculates the digest on the result.
    //a hyphen with the part count is added as a suffix.
    val expected = "${
      BinaryUtils.toBase64(DigestUtils.sha256(
        DigestUtils.sha256("Part1")  //testFile1
          + DigestUtils.sha256("Part2") //testFile2
      ))
    }-2"

    //files contain the exact content seen above
    val files = listOf(
      TestUtil.getTestFile(testInfo, "testFile1").toPath(),
      TestUtil.getTestFile(testInfo, "testFile2").toPath()
    )

    assertThat(DigestUtil.checksumMultipart(files, Algorithm.SHA256)).isEqualTo(expected)
  }
}
