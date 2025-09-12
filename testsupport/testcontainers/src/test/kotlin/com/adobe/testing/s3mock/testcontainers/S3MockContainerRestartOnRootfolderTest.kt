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
package com.adobe.testing.s3mock.testcontainers

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.output.Slf4jLogConsumer
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

internal class S3MockContainerRestartOnRootfolderTest : S3MockContainerTestBase() {
  private lateinit var s3Mock: S3MockContainer

  @BeforeEach
  fun setUp() {
    s3Mock = S3MockContainer(S3MOCK_VERSION).apply {
      withValidKmsKeys(TEST_ENC_KEYREF)
      withRetainFilesOnExit(true)
      withEnv("debug", "true")
      withInitialBuckets(INITIAL_BUCKET_NAMES.joinToString(","))
      withVolumeAsRoot(tempDir.absolutePath)
      start()
      followOutput(Slf4jLogConsumer(LOG))
    }

    val endpoint = s3Mock.httpsEndpoint
    s3Client = createS3ClientV2(endpoint)
  }

  @AfterEach
  fun tearDown() {
    if (this::s3Mock.isInitialized) {
      s3Mock.stop()
    }
  }

  companion object {
    private val tempDir: File =
      Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "s3mockFileStore").toFile()
  }
}
