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
package com.adobe.testing.s3mock.vectors

import org.apache.catalina.connector.Connector
import org.apache.commons.logging.LogFactory
import org.apache.tomcat.util.buf.EncodedSolidusHandling
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.ssl.SslBundle
import org.springframework.boot.ssl.SslBundleKey
import org.springframework.boot.ssl.SslStoreBundle
import org.springframework.boot.tomcat.SslConnectorCustomizer
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.security.KeyStore

/**
 * Adds dedicated HTTP and HTTPS connectors on the vectors ports (default 9092 / 9193) when the
 * `vectors` Spring profile is active. This lets `S3VectorsClient.endpointOverride(...)` use a
 * clean root URI separate from the S3 endpoints on 9090/9191.
 *
 * The HTTPS connector reuses the same self-signed keystore as the main S3 HTTPS port, read from
 * the `server.ssl.*` properties (defaulting to the bundled `s3mock.jks`).
 *
 * The existing S3 connectors registered by [com.adobe.testing.s3mock.S3MockConfiguration] are
 * unaffected.
 */
@Configuration
@Profile("vectors")
@EnableConfigurationProperties(S3VectorsProperties::class)
class VectorsTomcatConnectorsConfiguration(
  private val applicationContext: ApplicationContext,
) {
  @Value($$"${server.ssl.key-store:classpath:s3mock.jks}")
  private lateinit var keyStore: String

  @Value($$"${server.ssl.key-store-password:password}")
  private lateinit var keyStorePassword: String

  @Value($$"${server.ssl.key-alias:selfsigned}")
  private lateinit var keyAlias: String

  @Value($$"${server.ssl.key-password:password}")
  private lateinit var keyPassword: String

  @Value($$"${server.ssl.key-store-type:JKS}")
  private lateinit var keyStoreType: String

  @Bean
  fun vectorsConnectorCustomizer(vectorsProperties: S3VectorsProperties): WebServerFactoryCustomizer<TomcatServletWebServerFactory> =
    WebServerFactoryCustomizer { factory ->
      factory.addAdditionalConnectors(
        Connector().apply {
          port = vectorsProperties.httpPort
          // ARNs in the tagResource URL path contain slashes, which the SDK sends as %2F.
          // Tomcat 11 rejects %2F by default; "decode" allows it.
          encodedSolidusHandling = EncodedSolidusHandling.DECODE.value
        },
      )
      factory.addAdditionalConnectors(
        createHttpsConnector(vectorsProperties.httpsPort),
      )
    }

  private fun createHttpsConnector(port: Int): Connector {
    val ks =
      KeyStore.getInstance(keyStoreType).also {
        applicationContext.getResource(keyStore).inputStream.use { stream ->
          it.load(stream, keyStorePassword.toCharArray())
        }
      }
    val bundle =
      SslBundle.of(
        SslStoreBundle.of(ks, keyStorePassword, null),
        SslBundleKey.of(keyPassword, keyAlias),
      )
    return Connector("org.apache.coyote.http11.Http11NioProtocol").also { connector ->
      connector.port = port
      connector.encodedSolidusHandling = EncodedSolidusHandling.DECODE.value
      SslConnectorCustomizer(LogFactory.getLog(javaClass), connector, null)
        .customize(bundle, emptyMap())
    }
  }
}
