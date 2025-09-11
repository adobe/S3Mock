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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.store.KmsKeyStore
import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType
import org.springframework.core.env.Environment
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice

@SpringBootApplication
@ComponentScan(
  excludeFilters = [
    ComponentScan.Filter(
      type = FilterType.ANNOTATION,
      value = [Controller::class]
    ), ComponentScan.Filter(
      type = FilterType.ANNOTATION,
      value = [ControllerAdvice::class]
    ), ComponentScan.Filter(
      type = FilterType.ANNOTATION,
      value = [RestController::class]
    ), ComponentScan.Filter(
      type = FilterType.ANNOTATION,
      value = [RestControllerAdvice::class]
    )
  ]
)
class S3MockApplication(
  private val context: ConfigurableApplicationContext,
  private val kmsKeyStore: KmsKeyStore,
  private val environment: Environment,
  private val config: S3MockConfiguration
) {

  /**
   * Stops the server.
   */
  fun stop() {
    SpringApplication.exit(context, { 0 })
  }

  @get:Deprecated(
    """Using the S3Mock directly through Java is discouraged. Either run the JAR and start
        a separate JVM, or run the Docker container."""
  )
  val port: Int
    /**
     * Gets the Https server port.
     *
     * @return Https server port.
     */
    get() = environment.getRequiredProperty("local.server.port").toInt()

  @get:Deprecated(
    """Using the S3Mock directly through Java is discouraged. Either run the JAR and start
        a separate JVM, or run the Docker container."""
  )
  val httpPort: Int
    /**
     * Gets the Http server port.
     *
     * @return Http server port.
     */
    get() = config.getHttpConnector().localPort

  /**
   * Registers a valid KMS key reference on the mock server.
   *
   * @param keyRef A KMS Key Reference
   */
  @Deprecated(
    """Using the S3Mock directly through Java is discouraged. Either run the JAR and start
        a separate JVM, or run the Docker container."""
  )
  fun registerKMSKeyRef(keyRef: String) {
    kmsKeyStore.registerKMSKeyRef(keyRef)
  }

  companion object {
    const val DEFAULT_HTTPS_PORT: Int = 9191
    const val DEFAULT_HTTP_PORT: Int = 9090
    const val RANDOM_PORT: Int = 0

    const val DEFAULT_SERVER_SSL_KEY_STORE: String = "classpath:s3mock.jks"
    const val DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD: String = "password"
    const val DEFAULT_SERVER_SSL_KEY_ALIAS: String = "selfsigned"
    const val DEFAULT_SERVER_SSL_KEY_PASSWORD: String = "password"

    /**
     * Property name for passing the HTTPS port to use. Defaults to {@value DEFAULT_HTTPS_PORT}. If
     * set to {@value RANDOM_PORT}, a random port will be chosen.
     */
    const val PROP_HTTPS_PORT: String = "server.port"

    /**
     * Property name for passing the HTTP port to use. Defaults to  {@value DEFAULT_HTTP_PORT}. If set
     * to {@value RANDOM_PORT}, a random port will be chosen.
     */
    const val PROP_HTTP_PORT: String = "http.port"

    /**
     * Property name for passing the path to the keystore to use.
     * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_STORE}.
     */
    const val SERVER_SSL_KEY_STORE: String = "server.ssl.key-store"

    /**
     * Property name for passing the password for the keystore.
     * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD}.
     */
    const val SERVER_SSL_KEY_STORE_PASSWORD: String = "server.ssl.key-store-password"

    /**
     * Property name for specifying the key to use.
     * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_ALIAS}.
     */
    const val SERVER_SSL_KEY_ALIAS: String = "server.ssl.key-alias"

    /**
     * Property name for passing the password for the key.
     * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_PASSWORD}.
     */
    const val SERVER_SSL_KEY_PASSWORD: String = "server.ssl.key-password"

    /**
     * Property name for enabling the silent mode with logging set at WARN and without banner.
     */
    const val PROP_SILENT: String = "silent"

    /**
     * Main Class that starts the server using [.start].
     *
     * @param args Default command args.
     */
    @JvmStatic
    fun main(args: Array<String>) {
      start(*args)
    }

    /**
     * Starts the server.
     *
     * @param args in program args format, e.g. `"--server.port=0"`.
     *
     * @return the [S3MockApplication]
     */
    @JvmStatic
    fun start(vararg args: String): S3MockApplication {
      return start(mutableMapOf(), *args)
    }

    /**
     * Starts the server.
     *
     * @param properties properties to pass to the application in key-value format.
     * @param args in program args format, e.g. `"--server.port=0"`.
     *
     * @return the [S3MockApplication]
     */
    @JvmStatic
    fun start(
      properties: MutableMap<String, Any>,
      vararg args: String
    ): S3MockApplication {
      val defaults = mutableMapOf<String, Any>(
        PROP_HTTPS_PORT to DEFAULT_HTTPS_PORT,
        PROP_HTTP_PORT to DEFAULT_HTTP_PORT,
        // Specify the default SSL parameters here. Users can override them
        SERVER_SSL_KEY_STORE to DEFAULT_SERVER_SSL_KEY_STORE,
        SERVER_SSL_KEY_STORE_PASSWORD to DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD,
        SERVER_SSL_KEY_ALIAS to DEFAULT_SERVER_SSL_KEY_ALIAS,
        SERVER_SSL_KEY_PASSWORD to DEFAULT_SERVER_SSL_KEY_PASSWORD
      )

      var bannerMode = Banner.Mode.CONSOLE

      if (properties.remove(PROP_SILENT)?.toString()?.toBoolean() == true) {
        defaults["logging.level.root"] = "WARN"
        bannerMode = Banner.Mode.OFF
      }

      val ctx =
        SpringApplicationBuilder(S3MockApplication::class.java)
          .properties(defaults)
          .properties(properties)
          .bannerMode(bannerMode)
          .run(*args)

      return ctx.getBean(S3MockApplication::class.java)
    }
  }
}
