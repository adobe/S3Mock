/*
 *  Copyright 2017-2022 Adobe.
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

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest
import com.amazonaws.services.s3.model.MultipartUpload
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.utils.AttributeMap
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.net.Socket
import java.net.URI
import java.security.KeyManagementException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.Random
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.function.Consumer
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.X509ExtendedTrustManager
import kotlin.Array
import kotlin.ByteArray
import kotlin.Int
import kotlin.RuntimeException
import kotlin.String
import kotlin.arrayOf

/**
 * Base type for S3 Mock integration tests. Sets up S3 Client, Certificates, initial Buckets, etc.
 */
abstract class S3TestBase {
  var s3Client: AmazonS3? = null
  var s3ClientV2: S3Client? = null

  /**
   * Configures the S3-Client to be used in the Test. Sets the SSL context to accept untrusted SSL
   * connections.
   */
  @BeforeEach
  open fun prepareS3Client() {
    s3Client = defaultTestAmazonS3ClientBuilder().build()
    s3ClientV2 = createS3ClientV2()
  }

  protected fun defaultTestAmazonS3ClientBuilder(): AmazonS3ClientBuilder {
    return AmazonS3ClientBuilder.standard()
      .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(accessKeyId, secretAccessKey)))
      .withClientConfiguration(ignoringInvalidSslCertificates(ClientConfiguration()))
      .withEndpointConfiguration(
        EndpointConfiguration(serviceEndpoint, s3Region)
      )
      .enablePathStyleAccess()
  }

  protected val serviceEndpoint: String
    get() = s3Endpoint ?: "https://$host:$port"

  protected fun createS3ClientV2(): S3Client {
    return S3Client.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .endpointOverride(URI.create(serviceEndpoint))
      .httpClient(
        UrlConnectionHttpClient.builder().buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        )
      )
      .build()
  }

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    for (bucket in s3Client!!.listBuckets()) {
      if (!INITIAL_BUCKET_NAMES.contains(bucket.name)) {
        s3Client!!.listMultipartUploads(ListMultipartUploadsRequest(bucket.name))
          .multipartUploads.forEach(Consumer { upload: MultipartUpload ->
            s3Client!!.abortMultipartUpload(
              AbortMultipartUploadRequest(
                bucket.name, upload.key,
                upload.uploadId
              )
            )
          })
        s3Client!!.listObjects(bucket.name).objectSummaries.forEach(
          Consumer { `object`: S3ObjectSummary ->
            s3Client!!.deleteObject(
              bucket.name,
              `object`.key
            )
          })
        s3Client!!.deleteBucket(bucket.name)
      }
    }
  }

  private val s3Endpoint: String?
    get() = System.getProperty("it.s3mock.endpoint", null)
  private val accessKeyId: String
    get() = System.getProperty("it.s3mock.access.key.id", "foo")
  private val secretAccessKey: String
    get() = System.getProperty("it.s3mock.secret.access.key", "bar")
  private val s3Region: String
    get() = System.getProperty("it.s3mock.region", "us-east-1")
  private val port: Int
    get() = Integer.getInteger("it.s3mock.port_https", 9191)
    protected val host: String
    get() = System.getProperty("it.s3mock.host", "localhost")
  protected val httpPort: Int
    get() = Integer.getInteger("it.s3mock.port_http", 9090)

  private fun ignoringInvalidSslCertificates(clientConfiguration: ClientConfiguration):
    ClientConfiguration {
    clientConfiguration.apacheHttpClientConfig
      .withSslSocketFactory(
        SSLConnectionSocketFactory(
          createBlindlyTrustingSslContext(),
          NoopHostnameVerifier.INSTANCE
        )
      )
    return clientConfiguration
  }

  private fun createBlindlyTrustingSslContext(): SSLContext {
    return try {
      val sc = SSLContext.getInstance("TLS")
      sc.init(null, arrayOf<TrustManager>(object : X509ExtendedTrustManager() {
        override fun getAcceptedIssuers(): Array<X509Certificate> {
          return arrayOf()
        }

        override fun checkClientTrusted(certs: Array<X509Certificate>, authType: String) {
          // no-op
        }

        override fun checkClientTrusted(
          arg0: Array<X509Certificate>, arg1: String,
          arg2: SSLEngine
        ) {
          // no-op
        }

        override fun checkClientTrusted(
          arg0: Array<X509Certificate>, arg1: String,
          arg2: Socket
        ) {
          // no-op
        }

        override fun checkServerTrusted(
          arg0: Array<X509Certificate>, arg1: String,
          arg2: SSLEngine
        ) {
          // no-op
        }

        override fun checkServerTrusted(
          arg0: Array<X509Certificate>, arg1: String,
          arg2: Socket
        ) {
          // no-op
        }

        override fun checkServerTrusted(certs: Array<X509Certificate>, authType: String) {
          // no-op
        }
      }
      ), SecureRandom())
      sc
    } catch (e: NoSuchAlgorithmException) {
      throw RuntimeException("Unexpected exception", e)
    } catch (e: KeyManagementException) {
      throw RuntimeException("Unexpected exception", e)
    }
  }

  fun createTransferManager(): TransferManager {
    val threadFactory: ThreadFactory = object : ThreadFactory {
      private var threadCount = 1
      override fun newThread(r: Runnable): Thread {
        val thread = Thread(r)
        thread.name = "s3-transfer-" + threadCount++
        return thread
      }
    }
    return TransferManagerBuilder.standard()
      .withS3Client(s3Client)
      .withExecutorFactory { Executors.newFixedThreadPool(THREAD_COUNT, threadFactory) }
      .build()
  }

  fun randomInputStream(size: Int): InputStream {
    val content = ByteArray(size)
    Random().nextBytes(content)
    return ByteArrayInputStream(content)
  }

  companion object {
    val INITIAL_BUCKET_NAMES: Collection<String> = listOf("bucket-a", "bucket-b")
    const val TEST_ENC_KEY_ID = "valid-test-key-id"
    const val BUCKET_NAME = "my-demo-test-bucket"
    const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"
    const val TEST_WRONG_KEY_ID = "key-ID-WRONGWRONGWRONG"
    const val _1MB = 1024 * 1024
    const val _2MB = 2L * _1MB
    const val _5MB = 5L * _1MB
    const val _6MB = 6L * _1MB
    private const val _6BYTE = 6L
    private const val THREAD_COUNT = 50
  }
}
