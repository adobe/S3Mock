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

import static com.adobe.testing.s3mock.util.TestUtil.getTestFile;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class DigestUtilTest {

  @Test
  void testHexDigestOfMultipleFiles(TestInfo testInfo) throws IOException {
    //yes, this is correct - AWS calculates a Multipart digest by calculating the digest of every
    //file involved, and then calculates the digest on the result.
    //a hyphen with the part count is added as a suffix.
    var expected = DigestUtils.md5Hex(ArrayUtils.addAll(
        DigestUtils.md5("Part1"), //testFile1
        DigestUtils.md5("Part2") //testFile2
    )) + "-2";

    //files contain the exact content seen above
    var files = Arrays.asList(
        getTestFile(testInfo, "testFile1").toPath(),
        getTestFile(testInfo, "testFile2").toPath()
    );

    assertThat(DigestUtil.hexDigestMultipart(files)).as("Special hex digest doesn't match.")
        .isEqualTo(expected);
  }
}
