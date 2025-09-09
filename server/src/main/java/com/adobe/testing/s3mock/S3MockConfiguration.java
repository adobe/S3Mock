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

package com.adobe.testing.s3mock;

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.ctc.wstx.api.WstxOutputProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.buf.EncodedSolidusHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.filter.OrderedFormContentFilter;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.filter.CommonsRequestLoggingFilter;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Configuration
@EnableConfigurationProperties(S3MockProperties.class)
public class S3MockConfiguration implements WebMvcConfigurer {
  private Connector httpConnector;

  /**
   * Create a ServletWebServerFactory bean reconfigured for an additional HTTP port.
   *
   * @return webServerFactory bean reconfigured for an additional HTTP port
   */
  @Bean
  ServletWebServerFactory webServerFactory(S3MockProperties properties) {
    var factory = new TomcatServletWebServerFactory();
    factory.addConnectorCustomizers(connector -> {
      // Allow encoded slashes in URL
      connector.setEncodedSolidusHandling(EncodedSolidusHandling.DECODE.getValue());
      connector.setAllowBackslash(true);
    });
    factory.addAdditionalTomcatConnectors(createHttpConnector(properties.httpPort()));
    return factory;
  }

  private Connector createHttpConnector(int httpPort) {
    httpConnector = new Connector();
    httpConnector.setPort(httpPort);
    return httpConnector;
  }

  Connector getHttpConnector() {
    return httpConnector;
  }

  @Bean
  Filter kmsFilter(final KmsKeyStore kmsKeyStore,
      MappingJackson2XmlHttpMessageConverter messageConverter) {
    return new KmsValidationFilter(kmsKeyStore, messageConverter);
  }

  @Override
  public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
    configurer
        .defaultContentType(MediaType.APPLICATION_FORM_URLENCODED, MediaType.APPLICATION_XML);
    configurer.mediaType("xml", MediaType.TEXT_XML);
  }

  @Bean
  @Profile("debug")
  public CommonsRequestLoggingFilter logFilter() {
    var filter = new CommonsRequestLoggingFilter();
    filter.setIncludeQueryString(true);
    filter.setIncludeClientInfo(true);
    filter.setIncludePayload(true);
    filter.setMaxPayloadLength(10000);
    filter.setIncludeHeaders(true);
    return filter;
  }

  /**
   * Creates an HttpMessageConverter for XML.
   *
   * @return The configured {@link MappingJackson2XmlHttpMessageConverter}.
   */
  @Bean
  MappingJackson2XmlHttpMessageConverter messageConverter() {
    var mediaTypes = new ArrayList<MediaType>();
    mediaTypes.add(MediaType.APPLICATION_XML);
    mediaTypes.add(MediaType.APPLICATION_FORM_URLENCODED);
    mediaTypes.add(MediaType.APPLICATION_OCTET_STREAM);

    var xmlConverter = new MappingJackson2XmlHttpMessageConverter();
    xmlConverter.setSupportedMediaTypes(mediaTypes);
    XmlMapper xmlMapper = (XmlMapper) xmlConverter.getObjectMapper();
    xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    xmlMapper.enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE);
    xmlMapper.enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE);
    xmlMapper.enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
    xmlMapper.getFactory().getXMLOutputFactory()
        .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true);

    return xmlConverter;
  }

  @Bean
  OrderedFormContentFilter httpPutFormContentFilter() {
    return new OrderedFormContentFilter() {
      @Override
      protected boolean shouldNotFilter(HttpServletRequest request) {
        return true;
      }
    };
  }

  @Bean
  FaviconController faviconController() {
    return new FaviconController();
  }

  @Bean
  ObjectController fileStoreController(ObjectService objectService, BucketService bucketService) {
    return new ObjectController(bucketService, objectService);
  }

  @Bean
  BucketController bucketController(BucketService bucketService) {
    return new BucketController(bucketService);
  }

  @Bean
  MultipartController multipartController(BucketService bucketService,
      ObjectService objectService, MultipartService multipartService) {
    return new MultipartController(bucketService, objectService, multipartService);
  }

  @Bean
  S3MockExceptionHandler s3MockExceptionHandler() {
    return new S3MockExceptionHandler();
  }

  @Bean
  IllegalStateExceptionHandler illegalStateExceptionHandler() {
    return new IllegalStateExceptionHandler();
  }

  @Bean
  ObjectCannedAclHeaderConverter objectCannedAclHeaderConverter() {
    return new ObjectCannedAclHeaderConverter();
  }

  /**
   * Spring only provides an ObjectMapper that can serialize but not deserialize XML.
   */
  private XmlMapper xmlMapper() {
    var xmlMapper = XmlMapper.builder()
        .findAndAddModules()
        .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
        .enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
        .enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
        .build();
    xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    xmlMapper.getFactory()
        .getXMLOutputFactory()
        .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true);
    return xmlMapper;
  }

  @Bean
  TaggingHeaderConverter taggingHeaderConverter() {
    return new TaggingHeaderConverter(xmlMapper());
  }

  @Bean
  HttpRangeHeaderConverter httpRangeHeaderConverter() {
    return new HttpRangeHeaderConverter();
  }

  @Bean
  ObjectOwnershipHeaderConverter objectOwnershipHeaderConverter() {
    return new ObjectOwnershipHeaderConverter();
  }

  @Bean
  ChecksumModeHeaderConverter checksumModeHeaderConverter() {
    return new ChecksumModeHeaderConverter();
  }

  @Bean
  RegionConverter regionConverter() {
    return new RegionConverter();
  }

  /**
   * {@link ResponseEntityExceptionHandler} dealing with {@link S3Exception}s; Serializes them to
   * response output as suitable ErrorResponses.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">API Reference</a>
   */
  @ControllerAdvice
  static class S3MockExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(S3MockExceptionHandler.class);

    /**
     * Handles the given {@link S3Exception}.
     *
     * @param s3Exception {@link S3Exception} to be handled.
     *
     * @return A {@link ResponseEntity} representing the handled {@link S3Exception}.
     */
    @ExceptionHandler(S3Exception.class)
    public ResponseEntity<ErrorResponse> handleS3Exception(final S3Exception s3Exception) {
      LOG.debug("Responding with status {}: {}", s3Exception.getStatus(), s3Exception.getMessage(),
          s3Exception);

      var errorResponse = new ErrorResponse(
          s3Exception.getCode(),
          s3Exception.getMessage(),
          null,
          null
      );

      var headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_XML);
      if (s3Exception == S3Exception.NO_SUCH_KEY_DELETE_MARKER) {
        headers.set(X_AMZ_DELETE_MARKER, "true");
      }

      return ResponseEntity.status(s3Exception.getStatus()).headers(headers).body(errorResponse);
    }
  }

  /**
   * {@link ResponseEntityExceptionHandler} dealing with {@link IllegalStateException}s.
   * Serializes them to response output as a 500 Internal Server Error {@link ErrorResponse}.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html">API Reference</a>
   */
  @ControllerAdvice
  static class IllegalStateExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateExceptionHandler.class);

    /**
     * Handles the given {@link IllegalStateException}.
     *
     * @param exception {@link IllegalStateException} to be handled.
     *
     * @return A {@link ResponseEntity} representing the handled {@link IllegalStateException}.
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleS3Exception(IllegalStateException exception) {
      LOG.debug("Responding with status {}: {}", INTERNAL_SERVER_ERROR, exception.getMessage(),
          exception);

      var errorResponse = new ErrorResponse(
          "InternalError",
          "We encountered an internal error. Please try again.",
          null,
          null
      );

      var headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_XML);

      return ResponseEntity.internalServerError().headers(headers).body(errorResponse);
    }
  }
}
