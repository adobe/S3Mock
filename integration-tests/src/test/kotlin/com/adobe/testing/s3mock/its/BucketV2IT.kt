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
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.ExpirationStatus
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.LifecycleExpiration
import software.amazon.awssdk.services.s3.model.LifecycleRule
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter
import software.amazon.awssdk.services.s3.model.MFADelete
import software.amazon.awssdk.services.s3.model.MFADeleteStatus
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest
import java.util.concurrent.TimeUnit

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class BucketV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun createAndDeleteBucket(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    val bucketCreated = s3ClientV2.waiter()
      .waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build())
    val bucketCreatedResponse = bucketCreated.matched().response().get()
    assertThat(bucketCreatedResponse).isNotNull

    //does not throw exception if bucket exists.
    s3ClientV2.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())

    s3ClientV2.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    val bucketDeleted = s3ClientV2.waiter()
      .waitUntilBucketNotExists(HeadBucketRequest.builder().bucket(bucketName).build())
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getBucketLocation(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val bucketLocation = s3ClientV2.getBucketLocation(GetBucketLocationRequest.builder().bucket(bucketName).build())

    assertThat(bucketLocation.locationConstraint().toString()).isEqualTo("eu-west-1")
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getDefaultBucketVersioning(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isNull()
      assertThat(it.mfaDelete()).isNull()
    }
  }

  @Test
  @S3VerifiedTodo
  fun putAndGetBucketVersioning(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3ClientV2.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
    }
  }

  @Test
  @S3VerifiedTodo
  fun putAndGetBucketVersioning_suspended(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
      }
    }

    s3ClientV2.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
    }

    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.SUSPENDED)
      }
    }

    s3ClientV2.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.SUSPENDED)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2024, reason = "No real Mfa value")
  fun putAndGetBucketVersioning_mfa(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.putBucketVersioning {
      it.bucket(bucketName)
      it.mfa("fakeMfaValue")
      it.versioningConfiguration {
        it.status(BucketVersioningStatus.ENABLED)
        it.mfaDelete(MFADelete.ENABLED)
      }
    }

    s3ClientV2.getBucketVersioning {
      it.bucket(bucketName)
    }.also {
      assertThat(it.status()).isEqualTo(BucketVersioningStatus.ENABLED)
      assertThat(it.mfaDelete()).isEqualTo(MFADeleteStatus.ENABLED)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun duplicateBucketCreation(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    val bucketCreated = s3ClientV2.waiter()
      .waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build())
    bucketCreated.matched().response().get().also {
      assertThat(it).isNotNull
    }

    assertThatThrownBy {
      s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 409")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("BucketAlreadyOwnedByYou")

    s3ClientV2.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    val bucketDeleted = s3ClientV2.waiter()
      .waitUntilBucketNotExists(HeadBucketRequest.builder().bucket(bucketName).build())

    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun duplicateBucketDeletion(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    val bucketCreated = s3ClientV2.waiter()
      .waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build())
    bucketCreated.matched().response().get().also {
      assertThat(it).isNotNull
    }

    s3ClientV2.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    val bucketDeleted = s3ClientV2.waiter()
      .waitUntilBucketNotExists(HeadBucketRequest.builder().bucket(bucketName).build())
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
      assertThat(it).isInstanceOf(NoSuchBucketException::class.java)
    }

    assertThatThrownBy {
      s3ClientV2.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    }
      .isInstanceOf(AwsServiceException::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .asInstanceOf(InstanceOfAssertFactories.type(AwsServiceException::class.java))
      .extracting(AwsServiceException::awsErrorDetails)
      .extracting(AwsErrorDetails::errorCode)
      .isEqualTo("NoSuchBucket")
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getBucketLifecycle_notFound(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    val bucketCreated = s3ClientV2.waiter()
      .waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build())
    val bucketCreatedResponse = bucketCreated.matched().response()!!.get()
    assertThat(bucketCreatedResponse).isNotNull

    assertThatThrownBy {
      s3ClientV2.getBucketLifecycleConfiguration(
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

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun putGetDeleteBucketLifecycle(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())

    val bucketCreated = s3ClientV2.waiter()
      .waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build())
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

    s3ClientV2.putBucketLifecycleConfiguration(
      PutBucketLifecycleConfigurationRequest
        .builder()
        .bucket(bucketName)
        .lifecycleConfiguration(
          configuration
        )
        .build()
    )

    s3ClientV2.getBucketLifecycleConfiguration(
      GetBucketLifecycleConfigurationRequest
        .builder()
        .bucket(bucketName)
        .build()
    ).also {
      assertThat(it.rules()[0]).isEqualTo(configuration.rules()[0])
    }

    s3ClientV2.deleteBucketLifecycle(
      DeleteBucketLifecycleRequest.builder().bucket(bucketName).build()
    ).also {
      assertThat(it.sdkHttpResponse().statusCode()).isEqualTo(204)
    }


    // give AWS time to actually delete the lifecycleConfiguration, otherwise the following call
    // will not fail as expected...
    TimeUnit.SECONDS.sleep(3)

    assertThatThrownBy {
      s3ClientV2.getBucketLifecycleConfiguration(
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
