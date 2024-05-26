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
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.checksums.Algorithm
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
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
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
import software.amazon.awssdk.services.s3.model.S3Response
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.services.s3.model.UploadPartResponse
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration
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
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.function.Consumer
import java.util.stream.Stream
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.X509ExtendedTrustManager
import kotlin.random.Random


/**
 * Base type for S3 Mock integration tests. Sets up S3 Client, Certificates, initial Buckets, etc.
 */
internal abstract class S3TestBase {
  private val _s3Client: AmazonS3 = createS3ClientV1()
  private val _s3ClientV2: S3Client = createS3ClientV2()

  protected fun createS3ClientV1(endpoint: String = serviceEndpoint): AmazonS3 {
    return defaultTestAmazonS3ClientBuilder(endpoint).build()
  }

  protected fun defaultTestAmazonS3ClientBuilder(endpoint: String = serviceEndpoint): AmazonS3ClientBuilder {
    return AmazonS3ClientBuilder.standard()
      .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(accessKeyId, secretAccessKey)))
      .withClientConfiguration(ignoringInvalidSslCertificates(ClientConfiguration()))
      .withEndpointConfiguration(
        EndpointConfiguration(endpoint, s3Region)
      )
      .enablePathStyleAccess()
  }

  protected fun createTransferManagerV1(endpoint: String = serviceEndpoint,
      s3Client: AmazonS3 = createS3ClientV1(endpoint)): TransferManager {
    val threadFactory: ThreadFactory = object : ThreadFactory {
      private var threadCount = 1
      override fun newThread(r: Runnable): Thread {
        val thread = Thread(r)
        thread.name = "s3-transfer-${threadCount++}"
        return thread
      }
    }
    return TransferManagerBuilder
      .standard()
      .withS3Client(s3Client)
      .withExecutorFactory { Executors.newFixedThreadPool(THREAD_COUNT, threadFactory) }
      .build()
  }

  protected fun createS3ClientV2(endpoint: String = serviceEndpoint): S3Client {
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

  protected fun createS3AsyncClientV2(endpoint: String = serviceEndpoint): S3AsyncClient {
    return S3AsyncClient.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .forcePathStyle(true)
      .endpointOverride(URI.create(endpoint))
      .httpClient(NettyNioAsyncHttpClient
        .builder()
        .connectionTimeout(Duration.ofMinutes(5))
        .maxConcurrency(100)
        .buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        )
      )
      .multipartEnabled(true)
      .multipartConfiguration(MultipartConfiguration
        .builder()
        .thresholdInBytes((8 * MB).toLong())
        .build())
      .build()
  }

  protected fun createTransferManagerV2(endpoint: String = serviceEndpoint,
      s3AsyncClient: S3AsyncClient = createAutoS3CrtAsyncClientV2(endpoint)): S3TransferManager {
    return S3TransferManager.builder()
      .s3Client(s3AsyncClient)
      .build()
  }

  /**
   * Uses manual CRT client setup through AwsCrtAsyncHttpClient.builder()
   */
  protected fun createS3CrtAsyncClientV2(endpoint: String = serviceEndpoint): S3AsyncClient {
    return S3AsyncClient.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .forcePathStyle(true)
      .endpointOverride(URI.create(endpoint))
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
      .multipartEnabled(true)
      .multipartConfiguration(MultipartConfiguration.builder()
        .thresholdInBytes((8 * MB).toLong())
        .build())
      .build()
  }

  /**
   * Uses automated CRT client setup through S3AsyncClient.crtBuilder()
   */
  protected fun createAutoS3CrtAsyncClientV2(endpoint: String = serviceEndpoint): S3CrtAsyncClient {
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
      .endpointOverride(URI.create(endpoint))
      .targetThroughputInGbps(20.0)
      .minimumPartSizeInBytes((8 * MB).toLong())
      //S3Mock currently does not support checksum validation. See #1123
      .checksumValidationEnabled(false)
      .build() as S3CrtAsyncClient
  }

  protected fun createS3Presigner(endpoint: String = serviceEndpoint): S3Presigner {
    return S3Presigner.builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .build()
  }

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    for (bucket in _s3ClientV2.listBuckets().buckets()) {
      //Empty all buckets
      deleteMultipartUploads(bucket)
      deleteObjectsInBucket(bucket, isObjectLockEnabled(bucket))
      //Delete all "non-initial" buckets.
      if (!INITIAL_BUCKET_NAMES.contains(bucket.name())) {
        deleteBucket(bucket)
      }
    }
  }

  protected fun bucketName(testInfo: TestInfo): String {
    val methodName = testInfo.testMethod.get().name
    var normalizedName = methodName.lowercase().replace('_', '-')
    if (normalizedName.length > 50) {
      //max bucket name length is 63, shorten name to 50 since we add the timestamp below.
      normalizedName = normalizedName.substring(0,50)
    }
    val timestamp = Instant.now().nano
    val bucketName = "$normalizedName-$timestamp"
    LOG.info("Bucketname=$bucketName")
    return bucketName
  }

  fun givenBucketV1(testInfo: TestInfo): String {
    val bucketName = bucketName(testInfo)
    return givenBucketV1(bucketName)
  }

  private fun givenBucketV1(bucketName: String): String {
    _s3Client.createBucket(bucketName)
    return bucketName
  }

  fun givenRandomBucketV1(): String {
    return givenBucketV1(randomName)
  }

  private fun givenObjectV1(bucketName: String, key: String): PutObjectResult {
    val uploadFile = File(key)
    return _s3Client.putObject(PutObjectRequest(bucketName, key, uploadFile))
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
    _s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
    return bucketName
  }

  fun givenRandomBucketV2(): String {
    return givenBucketV2(randomName)
  }

  fun givenObjectV2(bucketName: String, key: String): PutObjectResponse {
    val uploadFile = File(key)
    return _s3ClientV2.putObject(
      software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
        .bucket(bucketName).key(key).build(),
      RequestBody.fromFile(uploadFile)
    )
  }

  fun deleteObjectV2(bucketName: String, key: String): DeleteObjectResponse {
    return _s3ClientV2.deleteObject(
      DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
    )
  }

  fun getObjectV2(bucketName: String, key: String): ResponseInputStream<GetObjectResponse> {
    return _s3ClientV2.getObject(
      GetObjectRequest.builder().bucket(bucketName).key(key).build()
    )
  }

  fun givenBucketAndObjectV2(testInfo: TestInfo, key: String): Pair<String, PutObjectResponse> {
    val bucketName = givenBucketV2(testInfo)
    val putObjectResponse = givenObjectV2(bucketName, key)
    return Pair(bucketName, putObjectResponse)
  }

  private fun deleteBucket(bucket: Bucket) {
    _s3ClientV2.deleteBucket(DeleteBucketRequest
      .builder()
      .bucket(bucket.name())
      .build()
    )
    val bucketDeleted = _s3ClientV2
      .waiter()
      .waitUntilBucketNotExists(HeadBucketRequest
        .builder()
        .bucket(bucket.name())
        .build()
      )
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
    }
  }

  private fun deleteObjectsInBucket(bucket: Bucket, objectLockEnabled: Boolean) {
    _s3ClientV2.listObjectsV2(
      ListObjectsV2Request.builder().bucket(bucket.name()).encodingType(EncodingType.URL).build()
    ).contents().forEach(
      Consumer { s3Object: S3Object ->
        if (objectLockEnabled) {
          //must remove potential legal hold, otherwise object can't be deleted
          _s3ClientV2.putObjectLegalHold(
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
        _s3ClientV2.deleteObject(
          DeleteObjectRequest.builder().bucket(bucket.name()).key(s3Object.key()).build()
        )
      })
  }

  private fun isObjectLockEnabled(bucket: Bucket): Boolean {
    return try {
      ObjectLockEnabled.ENABLED == _s3ClientV2.getObjectLockConfiguration(
        GetObjectLockConfigurationRequest
          .builder()
          .bucket(bucket.name())
          .build()
      )
        .objectLockConfiguration()
        .objectLockEnabled()
    } catch (e: S3Exception) {
      //#getObjectLockConfiguration throws S3Exception if not set
      false
    }
  }

  private fun deleteMultipartUploads(bucket: Bucket) {
    _s3ClientV2.listMultipartUploads(
      ListMultipartUploadsRequest
        .builder()
        .bucket(bucket.name())
        .build()
    ).uploads().forEach {
      _s3ClientV2.abortMultipartUpload(
        AbortMultipartUploadRequest
          .builder()
          .bucket(bucket.name())
          .key(it.key())
          .uploadId(it.uploadId())
          .build()
      )
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
  protected val randomName: String
    get() = UUID.randomUUID().toString()
  protected val serviceEndpoint: String
    get() = s3Endpoint ?: "https://$host:$port"
  protected val serviceEndpointHttp: String
    get() = s3Endpoint ?: "http://$host:$httpPort"
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

  fun randomInputStream(size: Int): InputStream {
    val content = ByteArray(size)
    Random.nextBytes(content)
    return ByteArrayInputStream(content)
  }

  fun verifyObjectContent(uploadFile: File, s3Object: com.amazonaws.services.s3.model.S3Object) {
    val uploadDigest = FileInputStream(uploadFile).use {
      DigestUtil.hexDigest(it)
    }

    s3Object.use {
      val downloadedDigest = DigestUtil.hexDigest(s3Object.objectContent)
      assertThat(uploadDigest)
        .isEqualTo(downloadedDigest)
        .`as`("Up- and downloaded Files should have equal digests")
    }
  }


  /**
   * Creates 5+MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun randomBytes(): ByteArray {
    return randomMBytes(_5MB.toInt() + Random.nextInt(_1MB))
  }

  /**
   * Creates exactly 5MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun random5MBytes(): ByteArray {
    return randomMBytes(_5MB.toInt())
  }

  private fun randomMBytes(size: Int): ByteArray {
    val bytes = ByteArray(size)
    Random.nextBytes(bytes)
    return bytes
  }

  @Throws(IOException::class)
  fun readStreamIntoByteArray(inputStream: InputStream): ByteArray {
    inputStream.use { it ->
      val outputStream = ByteArrayOutputStream(BUFFER_SIZE)
      val buffer = ByteArray(BUFFER_SIZE)
      var bytesRead: Int
      while (it.read(buffer).also { bytesRead = it } != -1) {
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

  fun ChecksumAlgorithm.toAlgorithm(): Algorithm = when (this) {
    ChecksumAlgorithm.SHA1 -> Algorithm.SHA1
    ChecksumAlgorithm.SHA256 -> Algorithm.SHA256
    ChecksumAlgorithm.CRC32 -> Algorithm.CRC32
    ChecksumAlgorithm.CRC32_C -> Algorithm.CRC32C
    else -> throw IllegalArgumentException("Unknown checksum algorithm")
  }

  fun S3Response.checksum(checksumAlgorithm: ChecksumAlgorithm): String? {
    fun S3Response.checksumSHA1(): String? {
      return when (this) {
        is GetObjectResponse -> this.checksumSHA1()
        is PutObjectResponse -> this.checksumSHA1()
        is HeadObjectResponse -> this.checksumSHA1()
        is UploadPartResponse -> this.checksumSHA1()
        is GetObjectAttributesResponse -> this.checksum().checksumSHA1()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }
    }

    fun S3Response.checksumSHA256(): String? {
      return when (this) {
        is GetObjectResponse -> this.checksumSHA256()
        is PutObjectResponse -> this.checksumSHA256()
        is HeadObjectResponse -> this.checksumSHA256()
        is UploadPartResponse -> this.checksumSHA256()
        is GetObjectAttributesResponse -> this.checksum().checksumSHA256()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }
    }

    fun S3Response.checksumCRC32(): String? {
      return when (this) {
        is GetObjectResponse -> this.checksumCRC32()
        is PutObjectResponse -> this.checksumCRC32()
        is HeadObjectResponse -> this.checksumCRC32()
        is UploadPartResponse -> this.checksumCRC32()
        is GetObjectAttributesResponse -> this.checksum().checksumCRC32()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }
    }

    fun S3Response.checksumCRC32C(): String? {
      return when (this) {
        is GetObjectResponse -> this.checksumCRC32C()
        is PutObjectResponse -> this.checksumCRC32C()
        is HeadObjectResponse -> this.checksumCRC32C()
        is UploadPartResponse -> this.checksumCRC32C()
        is GetObjectAttributesResponse -> this.checksum().checksumCRC32C()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }
    }

    return when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> this.checksumSHA1()
      ChecksumAlgorithm.SHA256 -> this.checksumSHA256()
      ChecksumAlgorithm.CRC32 -> this.checksumCRC32()
      ChecksumAlgorithm.CRC32_C -> this.checksumCRC32C()
      ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION -> "UNKNOWN_TO_SDK_VERSION"
    }
  }

  companion object {
    val INITIAL_BUCKET_NAMES: Collection<String> = listOf("bucket-a", "bucket-b")
    val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    const val TEST_ENC_KEY_ID = "valid-test-key-id"
    const val SAMPLE_FILE = "src/test/resources/sampleFile.txt"
    const val SAMPLE_FILE_LARGE = "src/test/resources/sampleFile_large.txt"
    const val TEST_IMAGE = "src/test/resources/test-image.png"
    const val TEST_IMAGE_LARGE = "src/test/resources/test-image_large.png"
    const val TEST_IMAGE_TIFF = "src/test/resources/test-image.tiff"
    const val UPLOAD_FILE_NAME = SAMPLE_FILE_LARGE
    const val TEST_WRONG_KEY_ID = "key-ID-WRONGWRONGWRONG"
    const val _1MB = 1024 * 1024
    const val _5MB = 5L * _1MB
    const val BUFFER_SIZE = 128 * 1024
    private const val THREAD_COUNT = 50
    private const val PREFIX = "prefix"
    val MAPPER = XmlMapper.builder().build()
    private val TEST_FILE_NAMES = listOf(
      SAMPLE_FILE,
      SAMPLE_FILE_LARGE,
      TEST_IMAGE,
      TEST_IMAGE_LARGE,
      TEST_IMAGE_TIFF,
    )

    @JvmStatic
    protected fun testFileNames(): Stream<String> {
      return Stream.of(*TEST_FILE_NAMES.toTypedArray())
    }

    @JvmStatic
    protected fun storageClasses(): Stream<StorageClass> {
      return StorageClass
        .entries
        .filter { it != StorageClass.UNKNOWN_TO_SDK_VERSION }
        .filter { it != StorageClass.SNOW }
        .filter { it != StorageClass.EXPRESS_ONEZONE }
        .filter { it != StorageClass.GLACIER }
        .filter { it != StorageClass.DEEP_ARCHIVE }
        .filter { it != StorageClass.OUTPOSTS }
        .map { it }
        .stream()
    }

    @JvmStatic
    protected fun checksumAlgorithms(): Stream<ChecksumAlgorithm> {
      return ChecksumAlgorithm
        .entries
        .filter { it != ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION }
        .map { it }
        .stream()
    }

    @JvmStatic
    protected fun charsSafe(): Stream<String> {
      return Stream.of(
        "$PREFIX${chars_safe_alphanumeric()}",
        "$PREFIX${chars_safe_special()}"
      )
    }

    @JvmStatic
    protected fun charsSafeKey(): String {
      return "$PREFIX${chars_safe_alphanumeric()}${chars_safe_special()}"
    }

    @JvmStatic
    protected fun charsSpecial(): Stream<String> {
      return Stream.of(
        "$PREFIX${chars_specialHandling()}",
        //"$PREFIX${chars_specialHandling_unicode()}" //TODO: some of these chars to not work.
      )
    }

    @JvmStatic
    protected fun charsSpecialKey(): String {
      return "$PREFIX${chars_specialHandling()}"
    }

    @JvmStatic
    protected fun charsToAvoid(): Stream<String> {
      return Stream.of(
        "$PREFIX${chars_toAvoid()}",
        //"$PREFIX${chars_toAvoid_unicode()}" //TODO: some of these chars to not work.
      )
    }

    @JvmStatic
    protected fun charsToAvoidKey(): String {
      return "$PREFIX${chars_toAvoid()}"
    }

    /**
     * Chars that are safe to use
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_safe_alphanumeric(): String {
      return listOf(
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
        "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
      ).joinToString(separator = "")
    }
    /**
     * Chars that are safe yet special
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_safe_special(): String {
      return listOf(
        "!", "-", "_", ".", "*", "'", "(", ")"
      ).joinToString(separator = "")
    }
    /**
     * Chars that might need special handling
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_specialHandling(): String {
      return listOf(
        "&", "$", "@", "=", ";", "/", ":", "+", " ", ",", "?"
      ).joinToString(separator = "")
    }
    /**
     * Unicode chars that might need special handling
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_specialHandling_unicode(): String {
      return listOf(
        "\u0000", "\u0001", "\u0002", "\u0003", "\u0004", "\u0005", "\u0006", "\u0007", "\u0008", "\u0009",
        "\u000A", "\u000B", "\u000C", "\u000D", "\u000E", "\u000F",
        "\u0010", "\u0011", "\u0012", "\u0013", "\u0014", "\u0015", "\u0016", "\u0017", "\u0018", "\u0019",
        "\u001A", "\u001B", "\u001C", "\u001D", "\u001E", "\u001F", "\u007F",
      ).joinToString(separator = "")
    }

    /**
     * Chars to avoid
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_toAvoid(): String {
      return listOf(
        "\\", "{", "^", "}", "%", "`", "]", "\"", ">", "[", "~", "<", "#", "|"
      ).joinToString(separator = "")
    }

    /**
     * Unicode chars to avoid
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_toAvoid_unicode(): String {
      return listOf(
        "\u0080", "\u0081", "\u0082", "\u0083", "\u0084", "\u0085", "\u0086", "\u0087", "\u0088", "\u0089",
        "\u008A", "\u008B", "\u008C", "\u008D", "\u008E", "\u008F",
        "\u0090", "\u0091", "\u0092", "\u0093", "\u0094", "\u0095", "\u0096", "\u0097", "\u0098", "\u0099",
        "\u009A", "\u009B", "\u009C", "\u009D", "\u009E", "\u009F",
        "\u00A0", "\u00A1", "\u00A2", "\u00A3", "\u00A4", "\u00A5", "\u00A6", "\u00A7", "\u00A8", "\u00A9",
        "\u00AA", "\u00AB", "\u00AC", "\u00AD", "\u00AE", "\u00AF",
        "\u00B0", "\u00B1", "\u00B2", "\u00B3", "\u00B4", "\u00B5", "\u00B6", "\u00B7", "\u00B8", "\u00B9",
        "\u00BA", "\u00BB", "\u00BC", "\u00BD", "\u00BE", "\u00BF",
        "\u00C0", "\u00C1", "\u00C2", "\u00C3", "\u00C4", "\u00C5", "\u00C6", "\u00C7", "\u00C8", "\u00C9",
        "\u00CA", "\u00CB", "\u00CC", "\u00CD", "\u00CE", "\u00CF",
        "\u00D0", "\u00D1", "\u00D2", "\u00D3", "\u00D4", "\u00D5", "\u00D6", "\u00D7", "\u00D8", "\u00D9",
        "\u00DA", "\u00DB", "\u00DC", "\u00DD", "\u00DE", "\u00DF",
        "\u0000", "\u0001", "\u0002", "\u0003", "\u0004", "\u0005", "\u0006", "\u0007", "\u0008", "\u0009",
        "\u000A", "\u000B", "\u000C", "\u000D", "\u000E", "\u000F",
        "\u0000", "\u0001", "\u0002", "\u0003", "\u0004", "\u0005", "\u0006", "\u0007", "\u0008", "\u0009",
        "\u000A", "\u000B", "\u000C", "\u000D", "\u000E", "\u000F",
      ).joinToString(separator = "")
    }
  }
}
