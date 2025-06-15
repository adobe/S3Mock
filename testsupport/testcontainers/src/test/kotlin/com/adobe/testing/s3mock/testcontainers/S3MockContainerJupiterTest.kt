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

import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.lang.String

/**
 * This shows how to let JUnit 5 Jupiter start and stop the S3MockContainer.
 * Tests are inherited from base class.
 */
@Testcontainers
internal class S3MockContainerJupiterTest : S3MockContainerTestBase() {
  // Container will be started before each test method and stopped after
  @Container
  private val s3Mock: S3MockContainer = S3MockContainer(S3MOCK_VERSION)
    .withValidKmsKeys(TEST_ENC_KEYREF)
    .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES))

  @BeforeEach
  fun setUp() {
    // Must create S3Client after S3MockContainer is started, otherwise we can't request the random
    // locally mapped port for the endpoint
    val endpoint = s3Mock.httpsEndpoint
    s3Client = createS3ClientV2(endpoint)
  }
}
