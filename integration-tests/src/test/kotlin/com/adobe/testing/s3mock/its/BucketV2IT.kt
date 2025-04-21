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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.InstanceOfAssertFactories
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AbortIncompleteMultipartUpload
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.ExpirationStatus
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest
import software.amazon.awssdk.services.s3.model.LifecycleExpiration
import software.amazon.awssdk.services.s3.model.LifecycleRule
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter
import software.amazon.awssdk.services.s3.model.MFADelete
import software.amazon.awssdk.services.s3.model.MFADeleteStatus
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class BucketV2IT : S3TestBase() {

  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `creating and deleting a bucket is successful`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket { it.bucket(bucketName) }

    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    val bucketCreatedResponse = bucketCreated.matched().response().get()
    assertThat(bucketCreatedResponse).isNotNull

    //does not throw exception if bucket exists.
    s3Client.headBucket { it.bucket(bucketName) }

    s3Client.deleteBucket { it.bucket(bucketName) }
    val bucketDeleted = s3Client.waiter().waitUntilBucketNotExists { it.bucket(bucketName) }
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }
  }

  @Test
  @S3VerifiedTodo
  fun `deleting a non-empty bucket fails`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    givenObject(bucketName, UPLOAD_FILE_NAME)
    assertThatThrownBy { s3Client.deleteBucket { it.bucket(bucketName) } }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 409")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("BucketNotEmpty")
  }

  @Test
  @S3VerifiedTodo
  fun `creating and listing multiple buckets is successful`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    givenBucket("${bucketName}-1")
    givenBucket("${bucketName}-2")
    givenBucket("${bucketName}-3")
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    val creationDate = Instant.now().minus(1, ChronoUnit.MINUTES)

    s3Client.listBuckets{
      it.prefix(bucketName)
    }.also {
      assertThat(it.hasBuckets()).isTrue
      //TODO: ListBuckets API currently ignores the prefix argument, see #2340
      it.buckets()
        .filter { b -> b.name().startsWith(bucketName) }.also { filteredBuckets ->
          assertThat(filteredBuckets.size).isEqualTo(3)
          assertThat(filteredBuckets.map { b -> b.name() })
            .containsExactlyInAnyOrder("${bucketName}-1", "${bucketName}-2", "${bucketName}-3")
          assertThat(filteredBuckets[0].creationDate()).isAfterOrEqualTo(creationDate)
          assertThat(filteredBuckets[1].creationDate()).isAfterOrEqualTo(creationDate)
          assertThat(filteredBuckets[2].creationDate()).isAfterOrEqualTo(creationDate)
        }
      assertThat(it.owner().displayName()).isEqualTo("s3-mock-file-store")
      assertThat(it.owner().id()).isEqualTo("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
    }
  }

  @Test
  @S3VerifiedFailure(year = 2025,
    reason = "Default buckets do not exist in S3.")
  fun `default buckets were created`(testInfo: TestInfo) {
    s3Client.listBuckets().also {
      assertThat(it.buckets())
        .hasSize(2)
        .extracting("name")
        .containsExactlyInAnyOrder(INITIAL_BUCKET_NAMES.first(), INITIAL_BUCKET_NAMES.last())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `get bucket location returns a result`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val bucketLocation = s3Client.getBucketLocation { it.bucket(bucketName) }

    assertThat(bucketLocation.locationConstraint().toString()).isEqualTo("eu-west-1")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `by default, bucket versioning is turned off`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isNull()
      assertThat(it.mfaDelete()).isNull()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `put bucket versioning works, get bucket versioning is returned correctly`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3Client.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `put bucket versioning works, suspending as well, get bucket versioning is returned correctly`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3Client.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
    }

    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.SUSPENDED)
      }
    }

    s3Client.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.SUSPENDED)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2024, reason = "No real Mfa value")
  fun `put bucket versioning with mfa works, get bucket versioning is returned correctly`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putBucketVersioning {
      it.bucket(bucketName)
      it.mfa("fakeMfaValue")
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
        it.mfaDelete(MFADelete.ENABLED)
      }
    }

    s3Client.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
      assertThat(it.mfaDelete()).isEqualTo(MFADeleteStatus.ENABLED)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `duplicate bucket creation returns the correct error`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket { it.bucket(bucketName) }

    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    bucketCreated.matched().response().get().also {
      assertThat(it).isNotNull
    }

    assertThatThrownBy {
      s3Client.createBucket { it.bucket(bucketName) }
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 409")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("BucketAlreadyOwnedByYou")

    s3Client.deleteBucket { it.bucket(bucketName) }
    val bucketDeleted = s3Client.waiter().waitUntilBucketNotExists { it.bucket(bucketName) }

    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `duplicate bucket deletion returns the correct error`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket { it.bucket(bucketName) }

    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    bucketCreated.matched().response().get().also {
      assertThat(it).isNotNull
    }

    s3Client.deleteBucket { it.bucket(bucketName) }
    val bucketDeleted = s3Client.waiter().waitUntilBucketNotExists { it.bucket(bucketName) }
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }

    assertThatThrownBy {
      s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchBucket")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `get bucket lifecycle returns error if not set`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket { it.bucket(bucketName) }

    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    val bucketCreatedResponse = bucketCreated.matched().response()!!.get()
    assertThat(bucketCreatedResponse).isNotNull

    assertThatThrownBy {
      s3Client.getBucketLifecycleConfiguration { it.bucket(bucketName) }
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchLifecycleConfiguration")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `put bucket lifecycle is successful, get bucket lifecycle returns the lifecycle, delete is successful`(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3Client.createBucket { it.bucket(bucketName) }

    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    bucketCreated.matched().response()!!.get().also {
      assertThat(it).isNotNull
    }

    val configuration = BucketLifecycleConfiguration
      .builder()
      .rules(
        LifecycleRule
          .builder()
          .id(bucketName)
          .abortIncompleteMultipartUpload(
            AbortIncompleteMultipartUpload
              .builder()
              .daysAfterInitiation(2)
              .build()
          )
          .expiration(
            LifecycleExpiration
              .builder()
              .days(2)
              .build()
          )
          .filter(LifecycleRuleFilter.fromPrefix("myprefix/"))
          .status(ExpirationStatus.ENABLED)
          .build()
      )
      .build()

    s3Client.putBucketLifecycleConfiguration {
      it.bucket(bucketName)
      it.lifecycleConfiguration(configuration)
    }

    s3Client.getBucketLifecycleConfiguration { it.bucket(bucketName) }.also {
      assertThat(it.rules()[0]).isEqualTo(configuration.rules()[0])
    }

    s3Client.deleteBucketLifecycle { it.bucket(bucketName) }.also {
      assertThat(it.sdkHttpResponse().statusCode()).isEqualTo(204)
    }


    // give AWS time to actually delete the lifecycleConfiguration, otherwise the following call
    // will not fail as expected...
    TimeUnit.SECONDS.sleep(3)

    assertThatThrownBy {
      s3Client.getBucketLifecycleConfiguration(
        GetBucketLifecycleConfigurationRequest.builder().bucket(bucketName).build()
      )
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchLifecycleConfiguration")
  }
}
