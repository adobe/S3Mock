/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.MultipartStore
import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import org.mockito.Mockito.mock

internal class MultipartServiceFuzzTest {
  private val iut = MultipartService(mock(BucketStore::class.java), mock(MultipartStore::class.java))

  @FuzzTest
  fun `fuzz part number validation`(data: FuzzedDataProvider) {
    val partNumber = data.consumeRemainingAsString()
    try {
      iut.verifyPartNumberLimits(partNumber)
    } catch (_: S3Exception) {
      // S3Exception is the expected outcome for invalid part numbers
    } catch (_: NumberFormatException) {
      // NumberFormatException is expected when the input is not a valid integer
    }
  }
}
