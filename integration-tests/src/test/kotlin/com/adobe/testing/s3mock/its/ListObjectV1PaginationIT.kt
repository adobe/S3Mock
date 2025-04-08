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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

@Deprecated("* AWS has deprecated SDK for Java v1, and will remove support EOY 2025.\n" +
  "    * S3Mock will remove usage of Java v1 early 2026.")
internal class ListObjectV1PaginationIT : S3TestBase() {
  val s3Client: AmazonS3 = createS3ClientV1()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun shouldTruncateAndReturnNextMarker(testInfo: TestInfo) {
    val bucketName = givenBucketWithTwoObjects(testInfo)
    val request = ListObjectsRequest().withBucketName(bucketName).withMaxKeys(1)

    val objectListing = s3Client.listObjects(request).also {
      assertThat(it.objectSummaries).hasSize(1)
      assertThat(it.maxKeys).isEqualTo(1)
      assertThat(it.nextMarker).isEqualTo("a")
      assertThat(it.isTruncated).isTrue
    }

    val continueRequest = ListObjectsRequest().withBucketName(bucketName).withMarker(objectListing.nextMarker)
    s3Client.listObjects(continueRequest).also {
      assertThat(it.objectSummaries.size).isEqualTo(1)
      assertThat(it.objectSummaries[0].key).isEqualTo("b")
    }
  }

  private fun givenBucketWithTwoObjects(testInfo: TestInfo): String {
    val bucketName = givenBucketV1(testInfo)
    s3Client.putObject(bucketName, "a", "")
    s3Client.putObject(bucketName, "b", "")
    return bucketName
  }
}
