/*
 *  Copyright 2017 Adobe.
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

import static java.util.stream.Collectors.toList;

import com.adobe.testing.s3mock.domain.Bucket;
import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.KMSKeyStore;
import com.adobe.testing.s3mock.dto.BatchDeleteRequest;
import com.adobe.testing.s3mock.dto.BatchDeleteResponse;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.util.ObjectRefConverter;
import com.adobe.testing.s3mock.util.RangeConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.catalina.connector.Connector;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.web.filter.OrderedHttpPutFormContentFilter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.converter.xml.MarshallingHttpMessageConverter;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.util.SocketUtils;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * File Store Application that mocks Amazon S3.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
public class S3MockApplication extends WebMvcConfigurerAdapter {
  private static final Logger LOG = Logger.getLogger(FileStoreController.class);

  private static final KMSKeyStore KEY_STORE = new KMSKeyStore();

  @Autowired
  private ConfigurableApplicationContext context;

  @Autowired
  private FileStore fileStore;

  @Value("${http.port}")
  private int httpPort;

  @Autowired
  private Environment environment;

  @Value("${initialBuckets:}")
  private String initialBuckets;

  /**
   * Main Class that starts the server using {@link #start(String...)}.
   *
   * @param args Default command args.
   */
  public static void main(final String[] args) {
    S3MockApplication.start();
  }

  /**
   * Creates the buckets that are expected to be available initially.
   *
   * @throws IOException not expected
   */
  @PostConstruct
  public void initBuckets() throws IOException {
    final List<String> buckets =
        Arrays.stream(initialBuckets.trim().split("[,; ]")).map(String::trim)
            .filter(s -> !s.isEmpty()).collect(toList());

    LOG.info("Creating initial buckets: " + buckets);

    for (final String bucketName : buckets) {
      LOG.info("Creating bucket: " + bucketName);
      fileStore.createBucket(bucketName);
    }
  }

  /**
   * @return servletContainer bean reconfigured using SSL
   */
  @Bean
  public EmbeddedServletContainerFactory servletContainer() {
    final TomcatEmbeddedServletContainerFactory tomcat =
        new TomcatEmbeddedServletContainerFactory();
    tomcat.addAdditionalTomcatConnectors(createHttpConnector());
    return tomcat;
  }

  /**
   * Returns the keyStore.
   *
   * @return the keyStore.
   */
  @Bean
  public KMSKeyStore kmsKeyStore() {
    return KEY_STORE;
  }

  /**
   * @return range bean for region (range request)
   */
  @Bean
  public RangeConverter rangeConverter() {
    return new RangeConverter();
  }

  @Bean
  public ObjectRefConverter objectRefConverter() {
    return new ObjectRefConverter();
  }

  private Connector createHttpConnector() {
    final Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
    connector.setPort(getHttpPort());
    return connector;
  }

  /**
   * Starts the server.
   *
   * @param args program args, e.g. {@code "--server.port=0"}.
   *
   * @return the {@link S3MockApplication}
   */
  public static S3MockApplication start(final String... args) {
    final ConfigurableApplicationContext appCtxt =
        SpringApplication.run(S3MockApplication.class, args);
    return appCtxt.getBean(S3MockApplication.class);
  }

  /**
   * Stops the server.
   */
  public void stop() {
    SpringApplication.exit(context, (ExitCodeGenerator) () -> 0);
  }

  /**
   * @return The HTTPS server port.
   */
  public int getPort() {
    return Integer.parseInt(environment.getProperty("local.server.port"));
  }

  /**
   * @return The server's HTTP port.
   */
  public synchronized int getHttpPort() {
    if (httpPort == 0) {
      httpPort = SocketUtils.findAvailableTcpPort();
    }

    return httpPort;
  }

  /**
   * @return the kms filter bean
   */
  @Bean
  public Filter kmsFilter() {
    return new KMSValidationFilter(kmsKeyStore());
  }

  /**
   * Registers a valid KMS key reference on the mock server.
   *
   * @param keyRef A KMS Key Reference
   */
  public void registerKMSKeyRef(final String keyRef) {
    kmsKeyStore().registerKMSKeyRef(keyRef);
  }

  @Override
  public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
    configurer.defaultContentType(MediaType.APPLICATION_FORM_URLENCODED);
    configurer.favorPathExtension(false);
    configurer.mediaType("xml", MediaType.TEXT_XML);
  }

  /**
   * @param xstreamMarshaller The fully configured {@link XStreamMarshaller}
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

  /**
   * @return The pre-configured {@link XStreamMarshaller}.
   */
  @Bean
  public XStreamMarshaller getXStreamMarshaller() {
    final XStreamMarshaller xstreamMarshaller = new XStreamMarshaller();

    xstreamMarshaller.setSupportedClasses(Bucket.class,
        Owner.class,
        ListAllMyBucketsResult.class,
        CopyPartResult.class,
        CopyObjectResult.class,
        ListBucketResult.class,
        InitiateMultipartUploadResult.class,
        ListPartsResult.class,
        CompleteMultipartUploadResult.class,
        BatchDeleteRequest.class,
        BatchDeleteResponse.class,
        ErrorResponse.class);

    xstreamMarshaller.setAnnotatedClasses(Bucket.class,
        Owner.class,
        CopyPartResult.class,
        ListAllMyBucketsResult.class,
        CopyObjectResult.class,
        ListBucketResult.class,
        InitiateMultipartUploadResult.class,
        ListPartsResult.class,
        CompleteMultipartUploadResult.class,
        BatchDeleteRequest.class,
        BatchDeleteResponse.class,
        ErrorResponse.class);

    return xstreamMarshaller;
  }

  /**
   * @return An {@link OrderedHttpPutFormContentFilter} that suppresses the FormContent filtering.
   */
  @Bean
  public OrderedHttpPutFormContentFilter httpPutFormContentFilter() {
    return new OrderedHttpPutFormContentFilter() {
      @Override
      protected boolean shouldNotFilter(final HttpServletRequest request) throws ServletException {
        return true;
      }
    };
  }
}
