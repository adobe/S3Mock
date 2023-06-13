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

package com.adobe.testing.s3mock.testcontainers;

import java.io.File;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.com.google.common.io.Files;

public class S3MockContainerRestartOnRootfolderTest extends S3MockContainerTestBase {

  private S3MockContainer s3Mock;
  private static final File tempDir = Files.createTempDir();

  @BeforeEach
  void setUp() {
    s3Mock = new S3MockContainer(S3MOCK_VERSION)
        .withValidKmsKeys(TEST_ENC_KEYREF)
        .withRetainFilesOnExit(true)
        .withEnv("debug", "true")
        .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES));
    s3Mock.withVolumeAsRoot(tempDir.getAbsolutePath());
    s3Mock.start();
    var logConsumer = new Slf4jLogConsumer(LOG);
    s3Mock.followOutput(logConsumer);
    // Must create S3Client after S3MockContainer is started, otherwise we can't request the random
    // locally mapped port for the endpoint
    var endpoint = s3Mock.getHttpsEndpoint();
    s3Client = createS3ClientV2(endpoint);
  }

  @AfterEach
  void tearDown() {
    s3Mock.stop();
  }
}
