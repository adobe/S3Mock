/*
 *  Copyright 2017-2024 Adobe.
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

import com.adobe.testing.s3mock.util.DigestUtil
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.internal.Constants.MB
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.Bucket
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.MultipartUpload
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.utils.AttributeMap
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.net.Socket
import java.net.URI
import java.security.KeyManagementException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.function.Consumer
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.X509ExtendedTrustManager


/**
 * Base type for S3 Mock integration tests. Sets up S3 Client, Certificates, initial Buckets, etc.
 */
internal abstract class S3TestBase {
  lateinit var s3Client: AmazonS3
  lateinit var s3ClientV2: S3Client
  lateinit var s3AsyncClientV2: S3AsyncClient
  lateinit var s3CrtAsyncClientV2: S3AsyncClient
  lateinit var autoS3CrtAsyncClientV2: S3AsyncClient
  lateinit var s3Presigner: S3Presigner

  /**
   * Configures the S3-Client to be used in the Test. Sets the SSL context to accept untrusted SSL
   * connections.
   */
  @BeforeEach
  open fun prepareS3Client() {
    s3Client = defaultTestAmazonS3ClientBuilder().build()
    s3ClientV2 = createS3ClientV2()
    s3AsyncClientV2 = createS3AsyncClientV2()
    s3CrtAsyncClientV2 = createS3CrtAsyncClientV2()
    autoS3CrtAsyncClientV2 = createAutoS3CrtAsyncClientV2()
    s3Presigner = createS3Presigner()
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

  protected val randomName: String
    get() = UUID.randomUUID().toString()

  protected fun bucketName(testInfo: TestInfo): String {
    val methodName = testInfo.testMethod.get().name
    var normalizedName = methodName.lowercase().replace('_', '-')
    if (normalizedName.length > 50) {
      //max bucket name length is 63, shorten name to 50 since we add the timestamp below.
      normalizedName = normalizedName.substring(0,50)
    }
    val timestamp = Instant.now().epochSecond
    val bucketName = "$normalizedName-$timestamp"
    LOG.info("Bucketname=$bucketName")
    return bucketName
  }

  protected val serviceEndpoint: String
    get() = s3Endpoint ?: "https://$host:$port"

  protected val serviceEndpointHttp: String
    get() = s3Endpoint ?: "http://$host:$httpPort"

  protected fun createS3ClientV2(): S3Client {
    return createS3ClientV2(serviceEndpoint)
  }

  protected fun createS3ClientV2(endpoint: String): S3Client {
    return S3Client.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .httpClient(
        ApacheHttpClient.builder().buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        )
      )
      .build()
  }

  protected fun createS3AsyncClientV2(): S3AsyncClient {
    return S3AsyncClient.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .forcePathStyle(true)
      .endpointOverride(URI.create(serviceEndpoint))
      .httpClient(NettyNioAsyncHttpClient
        .builder()
        .connectionTimeout(Duration.ofMinutes(5))
        .maxConcurrency(100)
        .buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        ))
      .build();
  }

  fun createTransferManagerV2(s3AsyncClient: S3AsyncClient): S3TransferManager {
    return S3TransferManager.builder()
      .s3Client(s3AsyncClient)
      .build();
  }

  /**
   * Uses manual CRT client setup through AwsCrtAsyncHttpClient.builder()
   */
  protected fun createS3CrtAsyncClientV2(): S3AsyncClient {
    return S3AsyncClient.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .forcePathStyle(true)
      .endpointOverride(URI.create(serviceEndpoint))
      .httpClient(AwsCrtAsyncHttpClient
        .builder()
        .connectionTimeout(Duration.ofMinutes(5))
        .maxConcurrency(100)
        .buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        )
      )
      .build();
  }

  /**
   * Uses automated CRT client setup through S3AsyncClient.crtBuilder()
   */
  protected fun createAutoS3CrtAsyncClientV2(): S3CrtAsyncClient {
    //using S3AsyncClient.crtBuilder does not work, can't get it to ignore custom SSL certificates.
    return S3AsyncClient.crtBuilder()
      .httpConfiguration {
        //this setting is ignored at runtime. Not sure why.
        it.trustAllCertificatesEnabled(true)
      }
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .forcePathStyle(true)
      //set endpoint to http(!)
      .endpointOverride(URI.create(serviceEndpointHttp))
      .targetThroughputInGbps(20.0)
      .minimumPartSizeInBytes((8 * MB).toLong())
      //S3Mock currently does not support checksum validation. See #1123
      .checksumValidationEnabled(false)
      .build() as S3CrtAsyncClient;
  }

  private fun createS3Presigner(): S3Presigner {
    return S3Presigner.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(serviceEndpoint))
      .build()
  }

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    for (bucket in s3ClientV2.listBuckets().buckets()) {
      //Empty all buckets
      deleteMultipartUploads(bucket)
      deleteObjectsInBucket(bucket, isObjectLockEnabled(bucket))
      //Delete all "non-initial" buckets.
      if (!INITIAL_BUCKET_NAMES.contains(bucket.name())) {
        deleteBucket(bucket)
      }
    }
  }

  fun givenBucketV1(testInfo: TestInfo): String {
    val bucketName = bucketName(testInfo)
    return givenBucketV1(bucketName)
  }

  private fun givenBucketV1(bucketName: String): String {
    s3Client.createBucket(bucketName)
    return bucketName
  }

  fun givenRandomBucketV1(): String {
    return givenBucketV1(randomName)
  }

  private fun givenObjectV1(bucketName: String, key: String): PutObjectResult {
    val uploadFile = File(key)
    return s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))
  }

  fun givenBucketAndObjectV1(testInfo: TestInfo, key: String): Pair<String, PutObjectResult> {
    val bucketName = givenBucketV1(testInfo)
    val putObjectResult = givenObjectV1(bucketName, key)
    return Pair(bucketName, putObjectResult)
  }

  fun givenBucketV2(testInfo: TestInfo): String {
    val bucketName = bucketName(testInfo)
    return givenBucketV2(bucketName)
  }

  fun givenBucketV2(bucketName: String): String {
    s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
    return bucketName
  }

  fun givenRandomBucketV2(): String {
    return givenBucketV2(randomName)
  }

  fun givenObjectV2(bucketName: String, key: String): PutObjectResponse {
    val uploadFile = File(key)
    return s3ClientV2.putObject(
      software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
        .bucket(bucketName).key(key).build(),
      RequestBody.fromFile(uploadFile)
    )
  }

  fun deleteObjectV2(bucketName: String, key: String): DeleteObjectResponse {
    return s3ClientV2.deleteObject(
      DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
    )
  }

  fun getObjectV2(bucketName: String, key: String): ResponseInputStream<GetObjectResponse> {
    return s3ClientV2.getObject(
      GetObjectRequest.builder().bucket(bucketName).key(key).build()
    )
  }

  fun givenBucketAndObjectV2(testInfo: TestInfo, key: String): Pair<String, PutObjectResponse> {
    val bucketName = givenBucketV2(testInfo)
    val putObjectResponse = givenObjectV2(bucketName, key)
    return Pair(bucketName, putObjectResponse)
  }

  private fun deleteBucket(bucket: Bucket) {
    s3ClientV2.deleteBucket(DeleteBucketRequest.builder().bucket(bucket.name()).build())
    val bucketDeleted = s3ClientV2.waiter()
      .waitUntilBucketNotExists(HeadBucketRequest.builder().bucket(bucket.name()).build())
    val bucketDeletedResponse = bucketDeleted.matched().exception().get()
    assertThat(bucketDeletedResponse).isNotNull
  }

  private fun deleteObjectsInBucket(bucket: Bucket, objectLockEnabled: Boolean) {
    s3ClientV2.listObjectsV2(
      ListObjectsV2Request.builder().bucket(bucket.name()).encodingType(EncodingType.URL).build()
    ).contents().forEach(
      Consumer { s3Object: S3Object ->
        if (objectLockEnabled) {
          //must remove potential legal hold, otherwise object can't be deleted
          s3ClientV2.putObjectLegalHold(
            PutObjectLegalHoldRequest
              .builder()
              .bucket(bucket.name())
              .key(s3Object.key())
              .legalHold(
                ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build()
              )
              .build()
          )
        }
        s3ClientV2.deleteObject(
          DeleteObjectRequest.builder().bucket(bucket.name()).key(s3Object.key()).build()
        )
      })
  }

  private fun isObjectLockEnabled(bucket: Bucket): Boolean {
    return try {
      ObjectLockEnabled.ENABLED == s3ClientV2.getObjectLockConfiguration(
        GetObjectLockConfigurationRequest.builder().bucket(bucket.name()).build()
      ).objectLockConfiguration().objectLockEnabled()
    } catch (e: S3Exception) {
      //#getObjectLockConfiguration throws S3Exception if not set
      false
    }
  }

  private fun deleteMultipartUploads(bucket: Bucket) {
    s3ClientV2.listMultipartUploads(
      ListMultipartUploadsRequest.builder().bucket(bucket.name()).build()
    ).uploads().forEach(Consumer { upload: MultipartUpload ->
      s3ClientV2.abortMultipartUpload(
        AbortMultipartUploadRequest.builder().bucket(bucket.name()).key(upload.key())
          .uploadId(upload.uploadId()).build()
      )
    }
    )
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

  fun verifyObjectContent(uploadFile: File, s3Object: com.amazonaws.services.s3.model.S3Object) {
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val uploadDigest = DigestUtil.hexDigest(uploadFileIs)
    val downloadedDigest = DigestUtil.hexDigest(s3Object.objectContent)
    uploadFileIs.close()
    s3Object.close()
    assertThat(uploadDigest)
      .isEqualTo(downloadedDigest)
      .`as`("Up- and downloaded Files should have equal digests")
  }


  /**
   * Creates 5+MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun randomBytes(): ByteArray {
    val size = _5MB.toInt() + random.nextInt(_1MB)
    val bytes = ByteArray(size)
    random.nextBytes(bytes)
    return bytes
  }

  /**
   * Creates exactly 5MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun random5MBytes(): ByteArray {
    val size = _5MB.toInt()
    val bytes = ByteArray(size)
    random.nextBytes(bytes)
    return bytes
  }

  @Throws(IOException::class)
  fun readStreamIntoByteArray(inputStream: InputStream): ByteArray {
    inputStream.use { `in` ->
      val outputStream = ByteArrayOutputStream(BUFFER_SIZE)
      val buffer = ByteArray(BUFFER_SIZE)
      var bytesRead: Int
      while (`in`.read(buffer).also { bytesRead = it } != -1) {
        outputStream.write(buffer, 0, bytesRead)
      }
      outputStream.flush()
      return outputStream.toByteArray()
    }
  }

  fun concatByteArrays(arr1: ByteArray, arr2: ByteArray): ByteArray {
    val result = ByteArray(arr1.size + arr2.size)
    System.arraycopy(arr1, 0, result, 0, arr1.size)
    System.arraycopy(arr2, 0, result, arr1.size, arr2.size)
    return result
  }

  companion object {
    val INITIAL_BUCKET_NAMES: Collection<String> = listOf("bucket-a", "bucket-b")
    const val TEST_ENC_KEY_ID = "valid-test-key-id"
    const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"
    const val TEST_WRONG_KEY_ID = "key-ID-WRONGWRONGWRONG"
    const val _1MB = 1024 * 1024
    const val _2MB = 2L * _1MB
    const val _5MB = 5L * _1MB
    const val _6MB = 6L * _1MB
    private const val _6BYTE = 6L
    private const val THREAD_COUNT = 50
    val random = Random()
    const val BUFFER_SIZE = 128 * 1024
    val LOG: Logger = LoggerFactory.getLogger(this::class.java)
  }
}
