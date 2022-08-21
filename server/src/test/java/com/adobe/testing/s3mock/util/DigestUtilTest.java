/*
 *  Copyright 2017-2022 Adobe.
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

import static org.apache.commons.lang3.StringUtils.replace;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
    String expected = DigestUtils.md5Hex(ArrayUtils.addAll(
        DigestUtils.md5("Part1"), //testFile1
        DigestUtils.md5("Part2") //testFile2
    )) + "-2";

    //files contain the exact content seen above
    List<Path> files = Arrays.asList(
        getFile(testInfo, "testFile1").toPath(),
        getFile(testInfo, "testFile2").toPath()
    );

    assertThat(DigestUtil.hexDigestMultipart(files)).as("Special hex digest doesn't match.")
        .isEqualTo(expected);
  }

  private static File getFile(TestInfo testInfo, String name) {
    Class<?> testClass = testInfo.getTestClass().get();
    String packageName = testClass.getPackage().getName();
    String className = testClass.getSimpleName();
    String methodName = testInfo.getTestMethod().get().getName();
    String fileName =
        String.format("%s/%s_%s_%s", replace(packageName, ".", "/"), className, methodName, name);

    ClassLoader classLoader = testClass.getClassLoader();
    return new File(Objects.requireNonNull(classLoader.getResource(fileName)).getFile());
  }

}
