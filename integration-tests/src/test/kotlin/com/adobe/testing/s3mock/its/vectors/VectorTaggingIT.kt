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
package com.adobe.testing.s3mock.its.vectors

import com.adobe.testing.s3mock.its.S3TestBase
import com.adobe.testing.s3mock.its.S3VerifiedSuccess
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

internal class VectorTaggingIT : S3TestBase() {
  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `tagging a bucket and listing tags returns the added tag`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val arn = vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) }.vectorBucket().vectorBucketArn()

    vectorsClient.tagResource { it.resourceArn(arn).tags(mapOf("env" to "test")) }

    val tags = vectorsClient.listTagsForResource { it.resourceArn(arn) }.tags()
    assertThat(tags).containsEntry("env", "test")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `untagging a bucket removes the specified tag`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val arn = vectorsClient.getVectorBucket { it.vectorBucketName(bucketName) }.vectorBucket().vectorBucketArn()

    vectorsClient.tagResource { it.resourceArn(arn).tags(mapOf("env" to "test")) }
    vectorsClient.untagResource { it.resourceArn(arn).tagKeys(listOf("env")) }

    val tags = vectorsClient.listTagsForResource { it.resourceArn(arn) }.tags()
    assertThat(tags).doesNotContainKey("env")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `tagging an index and listing tags returns the added tag`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexArn = givenIndex(bucketName)

    vectorsClient.tagResource { it.resourceArn(indexArn).tags(mapOf("env" to "test")) }

    val tags = vectorsClient.listTagsForResource { it.resourceArn(indexArn) }.tags()
    assertThat(tags).containsEntry("env", "test")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `untagging an index removes the specified tag`(testInfo: TestInfo) {
    val bucketName = givenVectorBucket(testInfo)
    val indexArn = givenIndex(bucketName)

    vectorsClient.tagResource { it.resourceArn(indexArn).tags(mapOf("env" to "test")) }
    vectorsClient.untagResource { it.resourceArn(indexArn).tagKeys(listOf("env")) }

    val tags = vectorsClient.listTagsForResource { it.resourceArn(indexArn) }.tags()
    assertThat(tags).doesNotContainKey("env")
  }

  private fun givenIndex(bucketName: String): String =
    vectorsClient
      .createIndex {
        it.vectorBucketName(bucketName)
        it.indexName("test-index")
        it.dataType("float32")
        it.dimension(4)
        it.distanceMetric("cosine")
      }.indexArn()
}
