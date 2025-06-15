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

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.Bucket
import software.amazon.awssdk.utils.AttributeMap
import java.io.File
import java.lang.Boolean
import java.net.URI
import java.nio.file.Files
import java.time.Instant
import java.util.Locale
import java.util.stream.Collectors
import kotlin.Exception
import kotlin.String
import kotlin.Throws

/**
 * This class contains test and utility methods used for manual and JUnit 5 test cases.
 */
internal abstract class S3MockContainerTestBase {
  protected var s3Client: S3Client? = null

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  @Throws(Exception::class)
  fun testPutAndGetObject(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)

    s3Client!!.createBucket {
      it.bucket(bucketName)
    }
    s3Client!!.putObject(
      {
        it.bucket(bucketName)
        it.key(uploadFile.getName())
      },
      RequestBody.fromFile(uploadFile)
    )

    s3Client!!.getObject {
      it.bucket(bucketName)
      it.key(uploadFile.getName())
    }.use { response ->
      val uploadDigest = Files.newInputStream(uploadFile.toPath()).use {
        DigestUtil.hexDigest(it)
      }
      val downloadedDigest = DigestUtil.hexDigest(response)
      assertThat(uploadDigest).isEqualTo(downloadedDigest)
    }
  }

  /**
   * Creates a bucket, stores a file, lists the bucket.
   */
  @Test
  fun testPutObjectAndListBucket(testInfo: TestInfo) {
    val bucketName = bucketName(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)

    s3Client!!.createBucket {
      it.bucket(bucketName)
    }
    s3Client!!.putObject(
      {
        it.bucket(bucketName)
        it.key(uploadFile.getName())
      },
      RequestBody.fromFile(uploadFile)
    )

    val listObjectsV2Response = s3Client!!.listObjectsV2 {
      it.bucket(bucketName)
    }

    assertThat(listObjectsV2Response.contents()).hasSize(1)
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  fun defaultBucketsGotCreated() {
    val buckets = s3Client!!.listBuckets().buckets()
    val bucketNames = buckets.stream().map { obj: Bucket? -> obj!!.name() }
      .filter { o: String? -> INITIAL_BUCKET_NAMES.contains(o) }.collect(Collectors.toSet())

    assertThat(bucketNames).containsAll(INITIAL_BUCKET_NAMES)
  }

  protected fun createS3ClientV2(endpoint: String): S3Client {
    return S3Client.builder()
      .region(Region.of("us-east-1"))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar"))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .httpClient(
        UrlConnectionHttpClient.builder().buildWithDefaults(
          AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE)
            .build()
        )
      )
      .build()
  }

  protected fun bucketName(testInfo: TestInfo): String {
    val methodName = testInfo.testMethod.get().name
    var normalizedName = methodName.lowercase(Locale.getDefault()).replace('_', '-')
    if (normalizedName.length > 50) {
      //max bucket name length is 63, shorten name to 50 since we add the timestamp below.
      normalizedName = normalizedName.substring(0, 50)
    }
    val timestamp = Instant.now().epochSecond
    return "$normalizedName-$timestamp"
  }

  companion object {
    @JvmStatic
    protected val LOG: Logger = LoggerFactory.getLogger(S3MockContainerTestBase::class.java)

    // we set the system property when running in maven, use "latest" for unit tests in the IDE
    @JvmStatic
    protected val S3MOCK_VERSION: String = System.getProperty("s3mock.version", "latest")

    @JvmStatic
    protected val INITIAL_BUCKET_NAMES: MutableCollection<String> = mutableListOf<String>("bucket-a", "bucket-b")
    protected const val TEST_ENC_KEYREF: String = "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref"
    protected const val UPLOAD_FILE_NAME: String = "src/test/resources/sampleFile.txt"
  }
}
