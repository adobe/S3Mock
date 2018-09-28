/*
 *  Copyright 2017-2018 Adobe.
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
import static java.util.stream.Collectors.toList;

import com.adobe.testing.s3mock.domain.Bucket;
import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.KmsKeyStore;
import com.adobe.testing.s3mock.domain.Tag;
import com.adobe.testing.s3mock.dto.BatchDeleteRequest;
import com.adobe.testing.s3mock.dto.BatchDeleteResponse;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.util.ObjectRefConverter;
import com.adobe.testing.s3mock.util.RangeConverter;
import com.thoughtworks.xstream.XStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import org.apache.catalina.connector.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.filter.OrderedFormContentFilter;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.converter.xml.MarshallingHttpMessageConverter;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * File Store Application that mocks Amazon S3.
 */
@Configuration
@EnableAutoConfiguration(exclude = {SecurityAutoConfiguration.class})
@ComponentScan
public class S3MockApplication {

  public static final int DEFAULT_HTTPS_PORT = 9191;
  public static final int DEFAULT_HTTP_PORT = 9090;
  public static final int RANDOM_PORT = 0;

  /**
   * Property name for passing a comma separated list of buckets that are to be created at startup.
   */
  public static final String PROP_INITIAL_BUCKETS = "initialBuckets";

  /**
   * Property name for passing a root directory to use. If omitted a default temp-dir will be used.
   */
  public static final String PROP_ROOT_DIRECTORY = "root";

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
   * Property name for using either HTTPS or HTTP connections.
   */
  public static final String PROP_SECURE_CONNECTION = "secureConnection";

  /**
   * Property name for enabling the silent mode with logging set at WARN and without banner.
   */
  public static final String PROP_SILENT = "silent";

  private static final Logger LOG = LoggerFactory.getLogger(FileStoreController.class);

  @Autowired
  private ConfigurableApplicationContext context;

  @Autowired
  private FileStore fileStore;

  @Autowired
  private KmsKeyStore kmsKeyStore;

  @Autowired
  private Environment environment;

  @Autowired
  private Config config;

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
    defaults.put(S3MockApplication.PROP_HTTPS_PORT, DEFAULT_HTTPS_PORT);
    defaults.put(S3MockApplication.PROP_HTTP_PORT, DEFAULT_HTTP_PORT);

    Banner.Mode bannerMode = Banner.Mode.CONSOLE;

    if (Boolean.valueOf(String.valueOf(properties.remove(PROP_SILENT)))) {
      defaults.put("logging.level.root", "WARN");
      bannerMode = Banner.Mode.OFF;
    }

    final ConfigurableApplicationContext ctx =
        new SpringApplicationBuilder(S3MockApplication.class)
            .properties(defaults)
            .properties(properties)
            .bannerMode(bannerMode)
            .run(args);

    return ctx.getBean(S3MockApplication.class);
  }

  /**
   * Stops the server.
   */
  public void stop() {
    SpringApplication.exit(context, (ExitCodeGenerator) () -> 0);
  }

  /**
   * Gets the Https server port.
   *
   * @return Https server port.
   */
  public int getPort() {
    return Integer.parseInt(environment.getProperty("local.server.port"));
  }

  /**
   * Gets the Http server port.
   *
   * @return Http server port.
   */
  public int getHttpPort() {
    return config.getHttpConnector().getLocalPort();
  }

  /**
   * Registers a valid KMS key reference on the mock server.
   *
   * @param keyRef A KMS Key Reference
   */
  public void registerKMSKeyRef(final String keyRef) {
    kmsKeyStore.registerKMSKeyRef(keyRef);
  }

  /**
   * Creates the buckets that are expected to be available initially.
   *
   * @throws IOException not expected
   */
  @PostConstruct
  void initBuckets() throws IOException {
    final List<String> buckets =
        Arrays.stream(config.getInitialBuckets().trim().split("[,; ]")).map(String::trim)
            .filter(s -> !s.isEmpty()).collect(toList());

    LOG.info("Creating initial buckets: " + buckets);

    for (final String bucketName : buckets) {
      LOG.info("Creating bucket: " + bucketName);
      fileStore.createBucket(bucketName);
    }
  }

  @Configuration
  static class Config implements WebMvcConfigurer {

    @Value("${" + PROP_HTTP_PORT + "}")
    private int httpPort;

    @Value("${" + PROP_INITIAL_BUCKETS + ":}")
    private String initialBuckets;

    private Connector httpConnector;

    /**
     * Create a ServletWebServerFactory bean reconfigured for an additional HTTP port.
     *
     * @return webServerFactory bean reconfigured for an additional HTTP port
     */
    @Bean
    ServletWebServerFactory webServerFactory() {
      final TomcatServletWebServerFactory tomcat =
          new TomcatServletWebServerFactory();
      tomcat.addAdditionalTomcatConnectors(createHttpConnector());
      return tomcat;
    }

    private Connector createHttpConnector() {
      httpConnector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
      httpConnector.setPort(httpPort);
      return httpConnector;
    }

    private Connector getHttpConnector() {
      return httpConnector;
    }

    @Bean
    KmsKeyStore kmsKeyStore() {
      return new KmsKeyStore();
    }

    @Bean
    RangeConverter rangeConverter() {
      return new RangeConverter();
    }

    @Bean
    ObjectRefConverter objectRefConverter() {
      return new ObjectRefConverter();
    }

    @Bean
    Filter kmsFilter(final KmsKeyStore kmsKeyStore) {
      return new KmsValidationFilter(kmsKeyStore);
    }

    @Override
    public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
      configurer.defaultContentType(MediaType.APPLICATION_FORM_URLENCODED);
      configurer.favorPathExtension(false);
      configurer.mediaType("xml", MediaType.TEXT_XML);
    }

    /**
     * Creates a MarshallingHttpMessageConverter using the given {@code xstreamMarshaller}.
     *
     * @param xstreamMarshaller The fully configured {@link XStreamMarshaller}
     *
     * @return The configured {@link MarshallingHttpMessageConverter}.
     */
    @Bean
    public MarshallingHttpMessageConverter getMessageConverter(
        final XStreamMarshaller xstreamMarshaller) {
      final List<MediaType> mediaTypes = new ArrayList<>();
      mediaTypes.add(MediaType.APPLICATION_XML);
      mediaTypes.add(MediaType.APPLICATION_FORM_URLENCODED);

      final MarshallingHttpMessageConverter xmlConverter = new MarshallingHttpMessageConverter();
      xmlConverter.setSupportedMediaTypes(mediaTypes);

      xmlConverter.setMarshaller(xstreamMarshaller);
      xmlConverter.setUnmarshaller(xstreamMarshaller);

      return xmlConverter;
    }

    @Bean
    XStreamMarshaller getXStreamMarshaller() {
      final Class[] supportedClasses = {
          BatchDeleteRequest.class,
          BatchDeleteRequest.ObjectToDelete.class,
          BatchDeleteResponse.class,
          Bucket.class,
          CompleteMultipartUploadResult.class,
          CopyObjectResult.class,
          CopyPartResult.class,
          ErrorResponse.class,
          InitiateMultipartUploadResult.class,
          ListAllMyBucketsResult.class,
          ListBucketResult.class,
          ListBucketResultV2.class,
          ListMultipartUploadsResult.class,
          ListPartsResult.class,
          Owner.class,
          Tagging.class,
          Tag.class
      };

      final XStreamMarshaller xstreamMarshaller = new XStreamMarshaller() {
        @Override
        protected void customizeXStream(final XStream xstream) {
          XStream.setupDefaultSecurity(xstream);
          xstream.allowTypes(supportedClasses);
        }
      };

      xstreamMarshaller.setSupportedClasses(supportedClasses);
      xstreamMarshaller.setAnnotatedClasses(supportedClasses);

      return xstreamMarshaller;
    }

    @Bean
    OrderedFormContentFilter httpPutFormContentFilter() {
      return new OrderedFormContentFilter() {
        @Override
        protected boolean shouldNotFilter(final HttpServletRequest request) {
          return true;
        }
      };
    }

    @Bean()
    Cache fileStorePagingStateCache() {
      return new ConcurrentMapCache("fileStorePagingStateCache");
    }

    String getInitialBuckets() {
      return initialBuckets;
    }
  }
}
