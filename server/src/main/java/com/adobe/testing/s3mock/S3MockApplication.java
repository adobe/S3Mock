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

package com.adobe.testing.s3mock;

import static java.util.Collections.emptyMap;

import com.adobe.testing.s3mock.store.KmsKeyStore;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * File Store Application that mocks Amazon S3.
 */
@SpringBootApplication(exclude = {SecurityAutoConfiguration.class},
    /*
     * Also exclude ManagementWebSecurityAutoConfiguration, to prevent the
     * erroneous activation of the CsrfFilter, which would cause access denied
     * errors upon accesses when spring-boot-actuator is on the class path.
     * This may be due to a bug in Spring Boot 2.1.2+. For details see
     * https://github.com/adobe/S3Mock/issues/130
     */
    excludeName = {"org.springframework.boot.actuate.autoconfigure.security.servlet."
        + "ManagementWebSecurityAutoConfiguration"}
)
@ComponentScan(excludeFilters = {
    /*
     * TypeFilter to exclude classes with annotations that inherit {@link Component} to be
     * instantiated automatically by Spring so that we can still manually instantiate them through
     * a @Configuration class.
     */
    @ComponentScan.Filter(type = FilterType.ANNOTATION, value = Controller.class),
    @ComponentScan.Filter(type = FilterType.ANNOTATION, value = ControllerAdvice.class),
    @ComponentScan.Filter(type = FilterType.ANNOTATION, value = RestController.class),
    @ComponentScan.Filter(type = FilterType.ANNOTATION, value = RestControllerAdvice.class)
})
public class S3MockApplication {

  public static final int DEFAULT_HTTPS_PORT = 9191;
  public static final int DEFAULT_HTTP_PORT = 9090;
  public static final int RANDOM_PORT = 0;

  public static final String DEFAULT_SERVER_SSL_KEY_STORE = "classpath:s3mock.jks";
  public static final String DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD = "password";
  public static final String DEFAULT_SERVER_SSL_KEY_ALIAS = "selfsigned";
  public static final String DEFAULT_SERVER_SSL_KEY_PASSWORD = "password";

  /**
   * Property name for passing a comma separated list of buckets that are to be created at startup.
   */
  public static final String PROP_INITIAL_BUCKETS =
      "com.adobe.testing.s3mock.domain.initialBuckets";
  private static final String LEGACY_PROP_INITIAL_BUCKETS = "initialBuckets";

  /**
   * Property name for passing a root directory to use. If omitted a default temp-dir will be used.
   */
  public static final String PROP_ROOT_DIRECTORY = "com.adobe.testing.s3mock.domain.root";
  private static final String LEGACY_PROP_ROOT_DIRECTORY = "root";

  /**
   * Property name for passing the HTTPS port to use. Defaults to {@value DEFAULT_HTTPS_PORT}. If
   * set to {@value RANDOM_PORT}, a random port will be chosen.
   */
  public static final String PROP_HTTPS_PORT = "server.port";

  /**
   * Property name for passing the HTTP port to use. Defaults to  {@value DEFAULT_HTTP_PORT}. If set
   * to {@value RANDOM_PORT}, a random port will be chosen.
   */
  public static final String PROP_HTTP_PORT = "http.port";

  /**
   * Property name for passing the path to the keystore to use.
   * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_STORE}.
   */
  public static final String SERVER_SSL_KEY_STORE = "server.ssl.key-store";

  /**
   * Property name for passing the password for the keystore.
   * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD}.
   */
  public static final String SERVER_SSL_KEY_STORE_PASSWORD = "server.ssl.key-store-password";

  /**
   * Property name for specifying the key to use.
   * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_ALIAS}.
   */
  public static final String SERVER_SSL_KEY_ALIAS = "server.ssl.key-alias";

  /**
   * Property name for passing the password for the key.
   * Defaults to  {@value DEFAULT_SERVER_SSL_KEY_PASSWORD}.
   */
  public static final String SERVER_SSL_KEY_PASSWORD = "server.ssl.key-password";

  /**
   * Property name for using either HTTPS or HTTP connections.
   */
  public static final String PROP_SECURE_CONNECTION = "secureConnection";

  /**
   * Property name for enabling the silent mode with logging set at WARN and without banner.
   */
  public static final String PROP_SILENT = "silent";

  @Autowired
  private ConfigurableApplicationContext context;

  @Autowired
  private KmsKeyStore kmsKeyStore;

  @Autowired
  private Environment environment;

  @Autowired
  private S3MockConfiguration config;

  /**
   * Main Class that starts the server using {@link #start(String...)}.
   *
   * @param args Default command args.
   */
  public static void main(final String[] args) {
    S3MockApplication.start(args);
  }

  /**
   * Starts the server.
   *
   * @param args in program args format, e.g. {@code "--server.port=0"}.
   *
   * @return the {@link S3MockApplication}
   */
  public static S3MockApplication start(final String... args) {
    return start(emptyMap(), args);
  }

  /**
   * Starts the server.
   *
   * @param properties properties to pass to the application in key-value format.
   * @param args in program args format, e.g. {@code "--server.port=0"}.
   *
   * @return the {@link S3MockApplication}
   */
  public static S3MockApplication start(final Map<String, Object> properties,
      final String... args) {

    final Map<String, Object> defaults = new HashMap<>();
    defaults.put(PROP_HTTPS_PORT, DEFAULT_HTTPS_PORT);
    defaults.put(PROP_HTTP_PORT, DEFAULT_HTTP_PORT);

    // Specify the default SSL parameters here. Users can override them
    defaults.put(SERVER_SSL_KEY_STORE, DEFAULT_SERVER_SSL_KEY_STORE);
    defaults.put(SERVER_SSL_KEY_STORE_PASSWORD, DEFAULT_SERVER_SSL_KEY_STORE_PASSWORD);
    defaults.put(SERVER_SSL_KEY_ALIAS, DEFAULT_SERVER_SSL_KEY_ALIAS);
    defaults.put(SERVER_SSL_KEY_PASSWORD, DEFAULT_SERVER_SSL_KEY_PASSWORD);

    Banner.Mode bannerMode = Banner.Mode.CONSOLE;

    if (Boolean.parseBoolean(String.valueOf(properties.remove(PROP_SILENT)))) {
      defaults.put("logging.level.root", "WARN");
      bannerMode = Banner.Mode.OFF;
    }

    final ConfigurableApplicationContext ctx =
        new SpringApplicationBuilder(S3MockApplication.class)
            .properties(translateLegacyProperties(defaults))
            .properties(translateLegacyProperties(properties))
            .bannerMode(bannerMode)
            .run(args);

    return ctx.getBean(S3MockApplication.class);
  }

  /**
   * Hack to still support startup of S3Mock from Java, e.g. in a (J)Unit-Test using one of the
   * Unit test integration modules.
   * -----------
   * TODO: Remove ASAP as this breaks Spring's Externalized Configuration
   * https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.external-config
   * -----------
   * S3Mock is using @Configuration / @ConfigurationProperties for each package.
   * Before this was implemented, all properties were exposed without a package.
   * Translation of these properties should be done in "application.properties" as is standard with
   * Spring (Boot) applications. However, if S3Mock is used directly in a unit test from within a
   * Spring application, S3Mock reads the "application.properties" of the tested application instead
   * of its own "application.properties".
   * This is equivalent to:
   * com.adobe.testing.s3mock.httpPort=${http.port:}
   * com.adobe.testing.s3mock.domain.initialBuckets=${initialBuckets:}
   * com.adobe.testing.s3mock.domain.retainFilesOnExit=${retainFilesOnExit:}
   * com.adobe.testing.s3mock.domain.root=${root:}
   * com.adobe.testing.s3mock.domain.validKmsKeys=${validKmsKeys:}
   */
  private static Map<String, Object> translateLegacyProperties(Map<String, Object> properties) {
    // make incoming map mutable
    Map<String, Object> translated = new HashMap<>(properties);
    translateLegacyProperty(translated, PROP_ROOT_DIRECTORY, LEGACY_PROP_ROOT_DIRECTORY);
    translateLegacyProperty(translated, PROP_INITIAL_BUCKETS, LEGACY_PROP_INITIAL_BUCKETS);
    translateLegacyProperty(translated,
        "com.adobe.testing.s3mock.domain.retainFilesOnExit", "retainFilesOnExit");
    translateLegacyProperty(translated,
        "com.adobe.testing.s3mock.domain.validKmsKeys", "validKmsKeys");
    translateLegacyProperty(translated,
        "com.adobe.testing.s3mock.httpPort", "http.port");
    return translated;
  }

  private static void translateLegacyProperty(Map<String, Object> properties, String propertyName,
      String legacyPropertyName) {
    if (!properties.containsKey(propertyName)) {
      // start by looking at incoming properties
      if (properties.containsKey(legacyPropertyName)) {
        properties.put(propertyName, properties.get(legacyPropertyName));
      }
      // these may be overwritten by system properties
      String legacySystemProperty = System.getProperty(legacyPropertyName);
      if (legacySystemProperty != null) {
        properties.put(propertyName, legacySystemProperty);
      }
      // highest precedence to environment variables.
      String legacyEnvironmentProperty = System.getenv(legacyPropertyName);
      if (legacyEnvironmentProperty != null) {
        properties.put(propertyName, legacyEnvironmentProperty);
      }
    }
  }

  /**
   * Stops the server.
   */
  public void stop() {
    SpringApplication.exit(context, () -> 0);
  }

  /**
   * Gets the Https server port.
   *
   * @return Https server port.
   * @deprecated Using the S3Mock directly through Java is discouraged. Either run the JAR and start
   *     a separate JVM, or run the Docker container.
   */
  @Deprecated
  public int getPort() {
    return Integer.parseInt(environment.getProperty("local.server.port"));
  }

  /**
   * Gets the Http server port.
   *
   * @return Http server port.
   * @deprecated Using the S3Mock directly through Java is discouraged. Either run the JAR and start
   *     a separate JVM, or run the Docker container.
   */
  @Deprecated
  public int getHttpPort() {
    return config.getHttpServerConnector().getLocalPort();
  }

  /**
   * Registers a valid KMS key reference on the mock server.
   *
   * @param keyRef A KMS Key Reference
   * @deprecated Using the S3Mock directly through Java is discouraged. Either run the JAR and start
   *     a separate JVM, or run the Docker container.
   */
  @Deprecated
  public void registerKMSKeyRef(final String keyRef) {
    kmsKeyStore.registerKMSKeyRef(keyRef);
  }
}
