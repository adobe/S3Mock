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

package com.adobe.testing.s3mock.testsupport.common

import com.adobe.testing.s3mock.S3MockApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class S3MockStarterTest {

  /**
   * Tests startup and shutdown of S3MockApplication.
   */
  @Test
  fun testS3MockApplication() {
    val properties = mapOf(
      S3MockApplication.PROP_HTTPS_PORT to S3MockApplication.RANDOM_PORT.toString(),
      S3MockApplication.PROP_HTTP_PORT to S3MockApplication.RANDOM_PORT.toString(),
      S3MockApplication.PROP_INITIAL_BUCKETS to "bucket"
    )

    val s3MockApplication = S3MockStarterTestImpl(properties)
    s3MockApplication.start()

    assertThat(s3MockApplication.httpPort).isPositive()
    val buckets = s3MockApplication.createS3ClientV2().use { s3ClientV2 ->
      s3ClientV2.listBuckets().buckets()
    }
    assertThat(buckets[0].name()).isEqualTo("bucket")

    s3MockApplication.stop()
  }

  /**
   * Just needed to instantiate the S3MockStarter.
   * The instance provides an S3Client that is pre-configured to connect to the S3MockApplication.
   */
  private class S3MockStarterTestImpl(properties: Map<String, String>) : S3MockStarter(properties)
}
