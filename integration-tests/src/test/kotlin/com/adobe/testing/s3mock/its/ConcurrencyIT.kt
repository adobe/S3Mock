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

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.minutes

internal class ConcurrencyIT : S3TestBase() {
  private val s3Client: S3Client = createS3ClientKotlin()

  @Test
  fun `concurrent bucket puts, gets and deletes are successful`(testInfo: TestInfo) =
    runTest(timeout = 3.minutes) {
      val bucketName = givenBucket(testInfo)
      val totalRequests = 1000
      val maxConcurrency = 20
      val done = AtomicInteger(0)

      val limitedDispatcher = Dispatchers.IO.limitedParallelism(maxConcurrency)

      coroutineScope {
        (1..totalRequests)
          .map { i ->
            async(limitedDispatcher) {
              val key = "test/key-$i"

              val putResponse =
                s3Client.putObject(
                  PutObjectRequest {
                    bucket = bucketName
                    this.key = key
                    body = ByteStream.fromBytes(byteArrayOf())
                  },
                )
              assertThat(putResponse.eTag).isNotNull
              assertThat(putResponse.eTag!!.isNotBlank()).isTrue()

              val getResponse =
                s3Client.getObject(
                  GetObjectRequest {
                    bucket = bucketName
                    this.key = key
                  },
                ) {
                  it
                }
              assertThat(getResponse.eTag).isNotNull
              assertThat(getResponse.eTag!!.isNotBlank()).isTrue()

              s3Client.deleteObject(
                DeleteObjectRequest {
                  bucket = bucketName
                  this.key = key
                },
              )

              done.incrementAndGet()
            }
          }.awaitAll()
      }

      assertThat(done.get()).isEqualTo(totalRequests)
    }
}
