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

import static com.adobe.testing.s3mock.util.TestUtil.getFileFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsSignedChunkedEncodingInputStream;

class AwsChunkedDecodingInputStreamTest {

  @Test
  void testDecoding(TestInfo testInfo) throws IOException {
    doTest(testInfo, "sampleFile.txt");
    doTest(testInfo, "sampleFile_large.txt");
  }

  void doTest(TestInfo testInfo, String fileName) throws IOException {
    File sampleFile = getFileFromClasspath(testInfo, fileName);
    InputStream chunkedEncodingInputStream = AwsSignedChunkedEncodingInputStream
        .builder()
        .inputStream(Files.newInputStream(sampleFile.toPath()))
        .awsChunkSigner(new AwsS3V4ChunkSigner("signingKey".getBytes(),
            "dateTime",
            "keyPath"))
        .build();
    InputStream iut = new AwsChunkedDecodingInputStream(chunkedEncodingInputStream);
    assertThat(iut).hasSameContentAs(Files.newInputStream(sampleFile.toPath()));
  }
}
