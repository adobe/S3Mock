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

import aws.smithy.kotlin.runtime.net.url.Url
import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
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
import software.amazon.awssdk.services.s3.model.Bucket
import software.amazon.awssdk.services.s3.model.BucketType
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Response
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.services.s3.model.UploadPartResponse
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.utils.AttributeMap
import tel.schich.awss3postobjectpresigner.S3PostObjectPresigner
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.net.Socket
import java.net.URI
import java.nio.file.Path
import java.security.KeyManagementException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.Instant
import java.util.UUID
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
  private val s3Client = createS3Client()

  protected fun createHttpClient(): CloseableHttpClient =
    HttpClientBuilder
      .create()
      .setSSLContext(createBlindlyTrustingSslContext())
      .setDefaultRequestConfig(RequestConfig.custom().setExpectContinueEnabled(true).build())
      .build()

  protected fun createS3Client(
    endpoint: String = serviceEndpoint,
    chunkedEncodingEnabled: Boolean? = null,
  ): S3Client =
    S3Client
      .builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).serviceConfiguration {
        it.pathStyleAccessEnabled(true)
        it.chunkedEncodingEnabled(chunkedEncodingEnabled)
      }.endpointOverride(URI.create(endpoint))
      .httpClient(
        ApacheHttpClient.builder().buildWithDefaults(
          AttributeMap
            .builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build(),
        ),
      ).build()

  protected fun createS3ClientKotlin(endpoint: String = serviceEndpointHttp): aws.sdk.kotlin.services.s3.S3Client =
    aws.sdk.kotlin.services.s3.S3Client {
      region = s3Region
      credentialsProvider =
        aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider {
          accessKeyId = s3AccessKeyId
          secretAccessKey = s3SecretAccessKey
        }
      forcePathStyle = true
      endpointUrl =
        Url
          .parse(endpoint)
    }

  protected fun createS3AsyncClient(endpoint: String = serviceEndpoint): S3AsyncClient =
    S3AsyncClient
      .builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).forcePathStyle(true)
      .endpointOverride(URI.create(endpoint))
      .httpClient(
        NettyNioAsyncHttpClient
          .builder()
          .connectionTimeout(Duration.ofMinutes(5))
          .maxConcurrency(100)
          .buildWithDefaults(
            AttributeMap
              .builder()
              .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
              .build(),
          ),
      ).multipartEnabled(true)
      .build()

  protected fun createTransferManager(
    endpoint: String = serviceEndpoint,
    s3AsyncClient: S3AsyncClient = createAutoS3CrtAsyncClient(endpoint),
  ): S3TransferManager =
    S3TransferManager
      .builder()
      .s3Client(s3AsyncClient)
      .build()

  /**
   * Uses manual CRT client setup through AwsCrtAsyncHttpClient.builder()
   */
  protected fun createS3CrtAsyncClient(endpoint: String = serviceEndpoint): S3AsyncClient =
    S3AsyncClient
      .builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).forcePathStyle(true)
      .endpointOverride(URI.create(endpoint))
      .httpClient(
        AwsCrtAsyncHttpClient
          .builder()
          .connectionTimeout(Duration.ofMinutes(5))
          .maxConcurrency(100)
          .buildWithDefaults(
            AttributeMap
              .builder()
              .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
              .build(),
          ),
      ).multipartEnabled(true)
      .build()

  /**
   * Uses automated CRT client setup through S3AsyncClient.crtBuilder()
   */
  protected fun createAutoS3CrtAsyncClient(endpoint: String = serviceEndpoint): S3CrtAsyncClient {
    // using S3AsyncClient.crtBuilder does not work, can't get it to ignore custom SSL certificates.
    return S3AsyncClient
      .crtBuilder()
      .httpConfiguration {
        // this setting is ignored at runtime. Not sure why.
        it.trustAllCertificatesEnabled(true)
      }.region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).forcePathStyle(true)
      // set endpoint to http(!)
      .endpointOverride(URI.create(endpoint))
      .build() as S3CrtAsyncClient
  }

  protected fun createS3Presigner(endpoint: String = serviceEndpoint): S3Presigner =
    S3Presigner
      .builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .build()

  protected fun createS3PostObjectPresigner(endpoint: String = serviceEndpoint): S3PostObjectPresigner =
    S3PostObjectPresigner
      .builder()
      .region(Region.of(s3Region))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey)),
      ).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .build()

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  fun cleanupStores() {
    s3Client.listBuckets().buckets().forEach { bucket ->
      // Empty all buckets
      deleteMultipartUploads(bucket)
      deleteObjectsInBucket(bucket, isObjectLockEnabled(bucket))
      // Delete all "non-initial" buckets.
      if (bucket.name() !in INITIAL_BUCKET_NAMES) {
        deleteBucket(bucket)
      }
    }
  }

  protected fun bucketName(testInfo: TestInfo): String {
    val normalizedName =
      testInfo.testMethod
        .get()
        .name
        .lowercase()
        .replace('_', '-')
        .replace(' ', '-')
        .replace(',', '-')
        .replace('\'', '-')
        .replace('=', '-')
        .let { if (it.length > 50) it.take(50) else it }
    val bucketName = "$normalizedName-${Instant.now().nano}"
    LOG.info("Bucketname=$bucketName")
    return bucketName
  }

  fun givenBucket(testInfo: TestInfo): String = givenBucket(bucketName(testInfo))

  fun givenBucket(bucketName: String = randomName): String {
    s3Client.createBucket { it.bucket(bucketName) }
    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    val bucketCreatedResponse = bucketCreated.matched().response().get()
    assertThat(bucketCreatedResponse).isNotNull
    return bucketName
  }

  fun givenDirectoryBucket(bucketName: String = randomName): String {
    s3Client.createBucket {
      it.bucket(bucketName)
      it.createBucketConfiguration {
        it.bucket {
          it.type(BucketType.DIRECTORY)
        }
      }
    }
    val bucketCreated = s3Client.waiter().waitUntilBucketExists { it.bucket(bucketName) }
    val bucketCreatedResponse = bucketCreated.matched().response().get()
    assertThat(bucketCreatedResponse).isNotNull
    return bucketName
  }

  fun givenObject(
    bucketName: String,
    key: String,
    fileName: String? = null,
  ): PutObjectResponse {
    val uploadFile = File(fileName ?: key)
    return s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(uploadFile),
    )
  }

  fun deleteObject(
    bucketName: String,
    key: String,
  ): DeleteObjectResponse =
    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(key)
    }

  fun getObject(
    bucketName: String,
    key: String,
  ): ResponseInputStream<GetObjectResponse> =
    s3Client.getObject {
      it.bucket(bucketName)
      it.key(key)
    }

  fun givenBucketAndObject(
    testInfo: TestInfo,
    key: String,
  ): Pair<String, PutObjectResponse> {
    val bucketName = givenBucket(testInfo)
    val putObjectResponse = givenObject(bucketName, key)
    return bucketName to putObjectResponse
  }

  fun givenBucketAndObjects(
    testInfo: TestInfo,
    count: Int,
  ): Pair<String, List<String>> {
    val baseKey = randomName
    val bucketName = givenBucket(testInfo)
    val keys =
      (0 until count).map { i ->
        "$baseKey-$i"
      }
    keys.forEach { key ->
      givenObject(bucketName, key, UPLOAD_FILE_NAME)
    }
    return bucketName to keys
  }

  private fun deleteBucket(bucket: Bucket) {
    s3Client.deleteBucket {
      it.bucket(bucket.name())
    }
    val bucketDeleted =
      s3Client
        .waiter()
        .waitUntilBucketNotExists {
          it.bucket(bucket.name())
        }
    bucketDeleted.matched().exception().get().also {
      assertThat(it).isNotNull
    }
  }

  private fun deleteObjectsInBucket(
    bucket: Bucket,
    objectLockEnabled: Boolean,
  ) {
    s3Client
      .listObjectVersions {
        it.bucket(bucket.name())
        it.encodingType(EncodingType.URL)
      }.also {
        it.versions().forEach { objectVersion ->
          if (objectLockEnabled) {
            // must remove potential legal hold, otherwise object can't be deleted
            s3Client.putObjectLegalHold {
              it.bucket(bucket.name())
              it.key(objectVersion.key())
              it.versionId(objectVersion.versionId())
              it.legalHold {
                it.status(ObjectLockLegalHoldStatus.OFF)
              }
            }
          }
          s3Client.deleteObject {
            it.bucket(bucket.name())
            it.key(objectVersion.key())
            it.versionId(objectVersion.versionId())
          }
        }
        it.deleteMarkers().forEach { marker ->
          if (objectLockEnabled) {
            // must remove potential legal hold, otherwise object can't be deleted
            s3Client.putObjectLegalHold {
              it.bucket(bucket.name())
              it.key(marker.key())
              it.versionId(marker.versionId())
              it.legalHold {
                it.status(ObjectLockLegalHoldStatus.OFF)
              }
            }
          }
          s3Client.deleteObject {
            it.bucket(bucket.name())
            it.key(marker.key())
            it.versionId(marker.versionId())
          }
        }
      }
  }

  private fun isObjectLockEnabled(bucket: Bucket): Boolean =
    try {
      ObjectLockEnabled.ENABLED ==
        s3Client
          .getObjectLockConfiguration {
            it.bucket(bucket.name())
          }.objectLockConfiguration()
          .objectLockEnabled()
    } catch (e: S3Exception) {
      // #getObjectLockConfiguration throws S3Exception if not set
      false
    }

  private fun deleteMultipartUploads(bucket: Bucket) {
    s3Client
      .listMultipartUploads {
        it.bucket(bucket.name())
      }.uploads()
      .forEach { upload ->
        s3Client.abortMultipartUpload {
          it.bucket(bucket.name())
          it.key(upload.key())
          it.uploadId(upload.uploadId())
        }
      }
  }

  private val s3Endpoint: String?
    get() = System.getProperty("it.s3mock.endpoint", null)
  private val s3AccessKeyId: String
    get() = System.getProperty("it.s3mock.access.key.id", "foo")
  private val s3SecretAccessKey: String
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

  protected fun createBlindlyTrustingSslContext(): SSLContext =
    try {
      val sc = SSLContext.getInstance("TLS")
      sc.init(
        null,
        arrayOf<TrustManager>(
          object : X509ExtendedTrustManager() {
            override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()

            override fun checkClientTrusted(
              certs: Array<X509Certificate>,
              authType: String,
            ) {
              // no-op
            }

            override fun checkClientTrusted(
              arg0: Array<X509Certificate>,
              arg1: String,
              arg2: SSLEngine,
            ) {
              // no-op
            }

            override fun checkClientTrusted(
              arg0: Array<X509Certificate>,
              arg1: String,
              arg2: Socket,
            ) {
              // no-op
            }

            override fun checkServerTrusted(
              arg0: Array<X509Certificate>,
              arg1: String,
              arg2: SSLEngine,
            ) {
              // no-op
            }

            override fun checkServerTrusted(
              arg0: Array<X509Certificate>,
              arg1: String,
              arg2: Socket,
            ) {
              // no-op
            }

            override fun checkServerTrusted(
              certs: Array<X509Certificate>,
              authType: String,
            ) {
              // no-op
            }
          },
        ),
        SecureRandom(),
      )
      sc
    } catch (e: NoSuchAlgorithmException) {
      throw RuntimeException("Unexpected exception", e)
    } catch (e: KeyManagementException) {
      throw RuntimeException("Unexpected exception", e)
    }

  fun randomInputStream(size: Int): InputStream {
    val content = ByteArray(size)
    Random.nextBytes(content)
    return ByteArrayInputStream(content)
  }

  /**
   * Creates 5+MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun randomBytes(): ByteArray = randomMBytes(FIVE_MB + Random.nextInt(ONE_MB))

  /**
   * Creates exactly 5MB of random bytes to upload as a valid part
   * (all parts but the last must be at least 5MB in size)
   */
  fun random5MBytes(): ByteArray = randomMBytes(FIVE_MB)

  protected fun randomMBytes(size: Int): ByteArray {
    val bytes = ByteArray(size)
    Random.nextBytes(bytes)
    return bytes
  }

  @Throws(IOException::class)
  fun readStreamIntoByteArray(inputStream: InputStream): ByteArray {
    // Use Kotlin's native extension for InputStream to read all bytes idiomatically
    return inputStream.use { it.readBytes() }
  }

  fun concatByteArrays(
    arr1: ByteArray,
    arr2: ByteArray,
  ): ByteArray {
    // Idiomatic Kotlin: allocate once and copy using copyInto to avoid System.arraycopy
    val result = ByteArray(arr1.size + arr2.size)
    arr1.copyInto(result, destinationOffset = 0)
    arr2.copyInto(result, destinationOffset = arr1.size)
    return result
  }

  fun software.amazon.awssdk.checksums.spi.ChecksumAlgorithm.toAlgorithm(): ChecksumAlgorithm =
    ChecksumAlgorithm.fromValue(this.algorithmId())

  fun S3Response.checksum(checksumAlgorithm: ChecksumAlgorithm): String? {
    fun S3Response.checksumSHA1(): String? =
      when (this) {
        is GetObjectResponse -> this.checksumSHA1()
        is PutObjectResponse -> this.checksumSHA1()
        is HeadObjectResponse -> this.checksumSHA1()
        is UploadPartResponse -> this.checksumSHA1()
        is GetObjectAttributesResponse -> this.checksum().checksumSHA1()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }

    fun S3Response.checksumSHA256(): String? =
      when (this) {
        is GetObjectResponse -> this.checksumSHA256()
        is PutObjectResponse -> this.checksumSHA256()
        is HeadObjectResponse -> this.checksumSHA256()
        is UploadPartResponse -> this.checksumSHA256()
        is GetObjectAttributesResponse -> this.checksum().checksumSHA256()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }

    fun S3Response.checksumCRC32(): String? =
      when (this) {
        is GetObjectResponse -> this.checksumCRC32()
        is PutObjectResponse -> this.checksumCRC32()
        is HeadObjectResponse -> this.checksumCRC32()
        is UploadPartResponse -> this.checksumCRC32()
        is GetObjectAttributesResponse -> this.checksum().checksumCRC32()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }

    fun S3Response.checksumCRC32C(): String? =
      when (this) {
        is GetObjectResponse -> this.checksumCRC32C()
        is PutObjectResponse -> this.checksumCRC32C()
        is HeadObjectResponse -> this.checksumCRC32C()
        is UploadPartResponse -> this.checksumCRC32C()
        is GetObjectAttributesResponse -> this.checksum().checksumCRC32C()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }

    fun S3Response.checksumCRC64NVME(): String? =
      when (this) {
        is GetObjectResponse -> this.checksumCRC64NVME()
        is PutObjectResponse -> this.checksumCRC64NVME()
        is HeadObjectResponse -> this.checksumCRC64NVME()
        is UploadPartResponse -> this.checksumCRC64NVME()
        is GetObjectAttributesResponse -> this.checksum().checksumCRC64NVME()
        else -> throw RuntimeException("Unexpected response type ${this::class.java}")
      }

    return when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> this.checksumSHA1()
      ChecksumAlgorithm.SHA256 -> this.checksumSHA256()
      ChecksumAlgorithm.CRC32 -> this.checksumCRC32()
      ChecksumAlgorithm.CRC32_C -> this.checksumCRC32C()
      ChecksumAlgorithm.CRC64_NVME -> this.checksumCRC64NVME()
      ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION -> "UNKNOWN_TO_SDK_VERSION"
    }
  }

  companion object {
    const val WILDCARD = "*"
    val INITIAL_BUCKET_NAMES: Collection<String> = listOf("bucket-a", "bucket-b")
    val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    const val TEST_ENC_KEY_ID = "valid-test-key-id"
    const val SAMPLE_FILE = "src/test/resources/sampleFile.txt"
    const val SAMPLE_FILE_LARGE = "src/test/resources/sampleFile_large.txt"
    const val TEST_IMAGE = "src/test/resources/test-image.png"
    const val TEST_IMAGE_LARGE = "src/test/resources/test-image_large.png"
    const val TEST_IMAGE_TIFF = "src/test/resources/test-image.tiff"
    const val UPLOAD_FILE_NAME = SAMPLE_FILE_LARGE
    val UPLOAD_FILE = File(UPLOAD_FILE_NAME)
    val UPLOAD_FILE_PATH: Path = UPLOAD_FILE.toPath()
    val UPLOAD_FILE_LENGTH = UPLOAD_FILE.length()
    const val TEST_WRONG_KEY_ID = "key-ID-WRONGWRONGWRONG"
    const val ONE_MB = 1024 * 1024
    const val FIVE_MB = 5 * ONE_MB
    private const val PREFIX = "prefix"
    val MAPPER: XmlMapper =
      XmlMapper
        .builder()
        .addModule(KotlinModule.Builder().build())
        .findAndAddModules()
        .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
        .enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
        .enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
        .build()
        .apply {
          setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
          factory.xmlOutputFactory
            .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)
        }

    private val TEST_FILE_NAMES =
      listOf(
        SAMPLE_FILE,
        SAMPLE_FILE_LARGE,
        TEST_IMAGE,
        TEST_IMAGE_LARGE,
        TEST_IMAGE_TIFF,
      )

    @JvmStatic
    protected fun testFileNames(): Stream<String> = Stream.of(*TEST_FILE_NAMES.toTypedArray())

    @JvmStatic
    protected fun storageClasses(): Stream<StorageClass> =
      listOf(
        StorageClass.STANDARD,
        StorageClass.REDUCED_REDUNDANCY,
        StorageClass.STANDARD_IA,
        StorageClass.ONEZONE_IA,
        StorageClass.INTELLIGENT_TIERING,
        StorageClass.GLACIER,
      ).stream()

    @JvmStatic
    protected fun checksumAlgorithms(): Stream<software.amazon.awssdk.checksums.spi.ChecksumAlgorithm> =
      listOf(
        DefaultChecksumAlgorithm.SHA256,
        DefaultChecksumAlgorithm.SHA1,
        DefaultChecksumAlgorithm.CRC32,
        DefaultChecksumAlgorithm.CRC32C,
        DefaultChecksumAlgorithm.CRC64NVME,
      ).stream()

    @JvmStatic
    protected fun charsSafe(): Stream<String> =
      Stream.of(
        "$PREFIX${chars_safe_alphanumeric()}",
        "$PREFIX${chars_safe_special()}",
      )

    @JvmStatic
    protected fun charsSafeKey(): String = "$PREFIX${chars_safe_alphanumeric()}${chars_safe_special()}"

    @JvmStatic
    protected fun charsSpecial(): Stream<String> =
      Stream.of(
        "$PREFIX${chars_specialHandling()}",
        // "$PREFIX${chars_specialHandling_unicode()}" //TODO: some of these chars to not work.
      )

    @JvmStatic
    protected fun charsSpecialKey(): String = "$PREFIX${chars_specialHandling()}"

    @JvmStatic
    protected fun charsToAvoid(): Stream<String> =
      Stream.of(
        "$PREFIX${chars_toAvoid()}",
        // "$PREFIX${chars_toAvoid_unicode()}" //TODO: some of these chars to not work.
      )

    @JvmStatic
    protected fun charsToAvoidKey(): String = "$PREFIX${chars_toAvoid()}"

    /**
     * Chars that are safe to use
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_safe_alphanumeric(): String =
      listOf(
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n",
        "o",
        "p",
        "q",
        "r",
        "s",
        "t",
        "u",
        "v",
        "w",
        "x",
        "y",
        "z",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
      ).joinToString(separator = "")

    /**
     * Chars that are safe yet special
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_safe_special(): String =
      listOf(
        "!",
        "-",
        "_",
        ".",
        "*",
        "'",
        "(",
        ")",
      ).joinToString(separator = "")

    /**
     * Chars that might need special handling
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_specialHandling(): String =
      listOf(
        "&",
        "$",
        "@",
        "=",
        ";",
        "/",
        ":",
        "+",
        " ",
        ",",
        "?",
      ).joinToString(separator = "")

    /**
     * Unicode chars that might need special handling
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_specialHandling_unicode(): String =
      listOf(
        "\u0000",
        "\u0001",
        "\u0002",
        "\u0003",
        "\u0004",
        "\u0005",
        "\u0006",
        "\u0007",
        "\u0008",
        "\u0009",
        "\u000A",
        "\u000B",
        "\u000C",
        "\u000D",
        "\u000E",
        "\u000F",
        "\u0010",
        "\u0011",
        "\u0012",
        "\u0013",
        "\u0014",
        "\u0015",
        "\u0016",
        "\u0017",
        "\u0018",
        "\u0019",
        "\u001A",
        "\u001B",
        "\u001C",
        "\u001D",
        "\u001E",
        "\u001F",
        "\u007F",
      ).joinToString(separator = "")

    /**
     * Chars to avoid
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_toAvoid(): String =
      listOf(
        "\\",
        "{",
        "^",
        "}",
        "%",
        "`",
        "]",
        "\"",
        ">",
        "[",
        "~",
        "<",
        "#",
        "|",
      ).joinToString(separator = "")

    /**
     * Unicode chars to avoid
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
     */
    @JvmStatic
    private fun chars_toAvoid_unicode(): String =
      listOf(
        "\u0080",
        "\u0081",
        "\u0082",
        "\u0083",
        "\u0084",
        "\u0085",
        "\u0086",
        "\u0087",
        "\u0088",
        "\u0089",
        "\u008A",
        "\u008B",
        "\u008C",
        "\u008D",
        "\u008E",
        "\u008F",
        "\u0090",
        "\u0091",
        "\u0092",
        "\u0093",
        "\u0094",
        "\u0095",
        "\u0096",
        "\u0097",
        "\u0098",
        "\u0099",
        "\u009A",
        "\u009B",
        "\u009C",
        "\u009D",
        "\u009E",
        "\u009F",
        "\u00A0",
        "\u00A1",
        "\u00A2",
        "\u00A3",
        "\u00A4",
        "\u00A5",
        "\u00A6",
        "\u00A7",
        "\u00A8",
        "\u00A9",
        "\u00AA",
        "\u00AB",
        "\u00AC",
        "\u00AD",
        "\u00AE",
        "\u00AF",
        "\u00B0",
        "\u00B1",
        "\u00B2",
        "\u00B3",
        "\u00B4",
        "\u00B5",
        "\u00B6",
        "\u00B7",
        "\u00B8",
        "\u00B9",
        "\u00BA",
        "\u00BB",
        "\u00BC",
        "\u00BD",
        "\u00BE",
        "\u00BF",
        "\u00C0",
        "\u00C1",
        "\u00C2",
        "\u00C3",
        "\u00C4",
        "\u00C5",
        "\u00C6",
        "\u00C7",
        "\u00C8",
        "\u00C9",
        "\u00CA",
        "\u00CB",
        "\u00CC",
        "\u00CD",
        "\u00CE",
        "\u00CF",
        "\u00D0",
        "\u00D1",
        "\u00D2",
        "\u00D3",
        "\u00D4",
        "\u00D5",
        "\u00D6",
        "\u00D7",
        "\u00D8",
        "\u00D9",
        "\u00DA",
        "\u00DB",
        "\u00DC",
        "\u00DD",
        "\u00DE",
        "\u00DF",
        "\u0000",
        "\u0001",
        "\u0002",
        "\u0003",
        "\u0004",
        "\u0005",
        "\u0006",
        "\u0007",
        "\u0008",
        "\u0009",
        "\u000A",
        "\u000B",
        "\u000C",
        "\u000D",
        "\u000E",
        "\u000F",
        "\u0000",
        "\u0001",
        "\u0002",
        "\u0003",
        "\u0004",
        "\u0005",
        "\u0006",
        "\u0007",
        "\u0008",
        "\u0009",
        "\u000A",
        "\u000B",
        "\u000C",
        "\u000D",
        "\u000E",
        "\u000F",
      ).joinToString(separator = "")
  }
}
