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

package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

internal class ConcurrencyIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  /**
   * Test that there are no inconsistencies when multiple threads PUT, GET and DELETE objects in
   * the same bucket.
   */
  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No need to test S3 concurrency."
  )
  fun `concurrent bucket puts, gets and deletes are successful`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val runners = mutableListOf<Runner>()
    val pool = Executors.newFixedThreadPool(100)
    for (i in 1..100) {
      runners.add(Runner(bucketName, "test/key$i"))
    }
    val futures = pool.invokeAll(runners)
    assertThat(futures).hasSize(100).allSatisfy {
      assertThat(it.get()).isTrue
    }
    assertThat(DONE.get()).isEqualTo(100)
  }

  companion object {
    val LATCH = CountDownLatch(100)
    val DONE = AtomicInteger(0)
  }

  inner class Runner(val bucketName: String, val key: String) : Callable<Boolean> {
    override fun call(): Boolean {
      LATCH.countDown()
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(key)
        }, RequestBody.empty()
      ).also {
        assertThat(it.eTag()).isNotBlank
      }

      s3Client.getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().eTag()).isNotBlank
      }

      s3Client.deleteObject {
        it.bucket(bucketName)
        it.key(key)
      }.also {
        assertThat(it.deleteMarker()).isTrue
      }
      DONE.incrementAndGet()
      return true
    }
  }
}
