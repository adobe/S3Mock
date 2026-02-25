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
package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.InstanceOfAssertFactories
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

internal class BucketTaggingIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `get bucket tagging returns error if not set`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.getBucketTagging { it.bucket(bucketName) }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchTagSet")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `put and get bucket tagging succeeds`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("env" to "production")
    val tag2 = tag("team" to "platform")

    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging(Tagging.builder().tagSet(tag1, tag2).build())
    }

    assertThat(
      s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet()
    ).contains(tag1, tag2)
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `put get and delete bucket tagging succeeds`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("env" to "staging")
    val tag2 = tag("owner" to "team-a")

    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging(Tagging.builder().tagSet(tag1, tag2).build())
    }

    assertThat(
      s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet()
    ).contains(tag1, tag2)

    s3Client.deleteBucketTagging { it.bucket(bucketName) }

    assertThatThrownBy {
      s3Client.getBucketTagging { it.bucket(bucketName) }
    }.isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchTagSet")
  }

  @Test
  @S3VerifiedSuccess(year = 2026)
  fun `put bucket tagging replaces existing tags`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val tag1 = tag("env" to "dev")

    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging(Tagging.builder().tagSet(tag1).build())
    }

    val tag2 = tag("env" to "prod")
    s3Client.putBucketTagging {
      it.bucket(bucketName)
      it.tagging(Tagging.builder().tagSet(tag2).build())
    }

    val result = s3Client.getBucketTagging { it.bucket(bucketName) }.tagSet()
    assertThat(result).containsExactly(tag2)
    assertThat(result).doesNotContain(tag1)
  }

  private fun tag(pair: Pair<String, String>): Tag =
    Tag.builder().key(pair.first).value(pair.second).build()
}
