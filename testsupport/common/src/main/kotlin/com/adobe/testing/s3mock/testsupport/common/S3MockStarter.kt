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
package com.adobe.testing.s3mock.testsupport.common

import com.adobe.testing.s3mock.S3MockApplication
import com.adobe.testing.s3mock.S3MockApplication.Companion.start
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.utils.AttributeMap
import java.net.URI

/**
 * Helps configuring and starting the S3Mock app and provides a configured client for it.
 */
abstract class S3MockStarter protected constructor(properties: Map<String, Any>? = null) {
  protected var s3MockFileStore: S3MockApplication? = null
  protected val properties: MutableMap<String, Any> = defaultProps().apply {
    properties?.let(::putAll)
  }

  protected fun defaultProps(): MutableMap<String, Any> =
    mutableMapOf(
      S3MockApplication.PROP_HTTPS_PORT to "0",
      S3MockApplication.PROP_HTTP_PORT to "0"
    )

  /**
   * Creates an [S3Client] client instance that is configured to call the started S3Mock
   * server using HTTPS.
   *
   * @return The [S3Client] instance.
   */
  fun createS3ClientV2(): S3Client =
    S3Client.builder()
      .region(Region.of("us-east-1"))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar"))
      )
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(serviceEndpoint))
      .httpClient(
        UrlConnectionHttpClient.builder().buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build()
        )
      )
      .build()

  val port: Int
    get() = requireNotNull(s3MockFileStore).port

  val httpPort: Int
    get() = requireNotNull(s3MockFileStore).httpPort

  /**
   * Registers a valid KMS key reference in the mock server.
   *
   * @param keyRef A KMS Key Reference
   */
  fun registerKMSKeyRef(keyRef: String) {
    requireNotNull(s3MockFileStore).registerKMSKeyRef(keyRef)
  }

  /**
   * Returns endpoint URL for connecting to the mock server.
   *
   * @return endpoint URL.
   */
  val serviceEndpoint: String
    get() {
      val isSecureConnection = properties.getOrDefault(PROP_SECURE_CONNECTION, true) as Boolean
      return if (isSecureConnection) {
        "https://localhost:$port"
      } else {
        "http://localhost:$httpPort"
      }
    }

  fun start() {
    s3MockFileStore = start(properties)
  }

  fun stop() {
    s3MockFileStore?.stop()
  }

  abstract class BaseBuilder<T : S3MockStarter> {
    protected val arguments: MutableMap<String, Any> = mutableMapOf()

    fun withProperty(name: String, value: String): BaseBuilder<T> = apply {
      arguments[name] = value
    }

    fun withInitialBuckets(vararg initialBuckets: String): BaseBuilder<T> = apply {
      arguments[PROP_INITIAL_BUCKETS] = initialBuckets.joinToString(",")
    }

    fun withHttpsPort(httpsPort: Int): BaseBuilder<T> = apply {
      arguments[S3MockApplication.PROP_HTTPS_PORT] = httpsPort.toString()
    }

    fun withHttpPort(httpPort: Int): BaseBuilder<T> = apply {
      arguments[S3MockApplication.PROP_HTTP_PORT] = httpPort.toString()
    }

    fun withRootFolder(rootFolder: String): BaseBuilder<T> = apply {
      arguments[PROP_ROOT_DIRECTORY] = rootFolder
    }

    fun withRegion(region: String): BaseBuilder<T> = apply {
      arguments[PROP_REGION] = region
    }

    fun withValidKmsKeys(kmsKeys: String): BaseBuilder<T> = apply {
      arguments[PROP_VALID_KMS_KEYS] = kmsKeys
    }

    fun withRetainFilesOnExit(retainFilesOnExit: Boolean): BaseBuilder<T> = apply {
      arguments[PROP_RETAIN_FILES_ON_EXIT] = retainFilesOnExit
    }

    fun withSecureConnection(secureConnection: Boolean): BaseBuilder<T> = apply {
      arguments[PROP_SECURE_CONNECTION] = secureConnection
    }

    /**
     * Configures SSL parameters for the mock server.
     *
     * @param keyStore value for server.ssl.key-store
     * @param keyStorePassword value for server.ssl.key-store-password
     * @param keyAlias value for server.ssl.key-alias
     * @param keyPassword value for server.ssl.key-password
     *
     * @return this builder
     */
    fun withSslParameters(
      keyStore: String,
      keyStorePassword: String,
      keyAlias: String,
      keyPassword: String
    ): BaseBuilder<T> = apply {
      arguments[S3MockApplication.SERVER_SSL_KEY_STORE] = keyStore
      arguments[S3MockApplication.SERVER_SSL_KEY_STORE_PASSWORD] = keyStorePassword
      arguments[S3MockApplication.SERVER_SSL_KEY_ALIAS] = keyAlias
      arguments[S3MockApplication.SERVER_SSL_KEY_PASSWORD] = keyPassword
    }

    /**
     * Reduces logging level WARN and suppresses the startup banner.
     *
     * @return the builder
     */
    fun silent(): BaseBuilder<T> = apply {
      arguments[S3MockApplication.PROP_SILENT] = true
    }

    /**
     * Creates the instance.
     *
     * @return The configured instance.
     */
    abstract fun build(): T
  }

  companion object {
    const val PROP_INITIAL_BUCKETS: String = "com.adobe.testing.s3mock.store.initialBuckets"
    private const val PROP_ROOT_DIRECTORY = "com.adobe.testing.s3mock.store.root"
    private const val PROP_VALID_KMS_KEYS = "com.adobe.testing.s3mock.store.validKmsKeys"
    private const val PROP_REGION = "com.adobe.testing.s3mock.store.region"
    private const val PROP_RETAIN_FILES_ON_EXIT = "com.adobe.testing.s3mock.store.retainFilesOnExit"
    private const val PROP_SECURE_CONNECTION = "secureConnection"
  }
}
