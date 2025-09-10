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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER
import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import jakarta.servlet.Filter
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.servlet.filter.OrderedFormContentFilter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.filter.CommonsRequestLoggingFilter
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler

@Configuration
@EnableConfigurationProperties(ControllerProperties::class)
class ControllerConfiguration : WebMvcConfigurer {
  @Bean
  fun kmsFilter(
    kmsKeyStore: KmsKeyStore,
    messageConverter: MappingJackson2XmlHttpMessageConverter
  ): Filter {
    return KmsValidationFilter(kmsKeyStore, messageConverter)
  }

  override fun configureContentNegotiation(configurer: ContentNegotiationConfigurer) {
    configurer
      .defaultContentType(MediaType.APPLICATION_FORM_URLENCODED, MediaType.APPLICATION_XML)
    configurer.mediaType("xml", MediaType.TEXT_XML)
  }

  @Bean
  @Profile("debug")
  fun logFilter(): CommonsRequestLoggingFilter {
    val filter = CommonsRequestLoggingFilter()
    filter.setIncludeQueryString(true)
    filter.setIncludeClientInfo(true)
    filter.setIncludePayload(true)
    filter.setMaxPayloadLength(10000)
    filter.setIncludeHeaders(true)
    return filter
  }

  /**
   * Creates an HttpMessageConverter for XML.
   *
   * @return The configured [MappingJackson2XmlHttpMessageConverter].
   */
  @Bean
  fun messageConverter(): MappingJackson2XmlHttpMessageConverter {
    val mediaTypes = ArrayList<MediaType>()
    mediaTypes.add(MediaType.APPLICATION_XML)
    mediaTypes.add(MediaType.APPLICATION_FORM_URLENCODED)
    mediaTypes.add(MediaType.APPLICATION_OCTET_STREAM)

    val xmlConverter = MappingJackson2XmlHttpMessageConverter()
    xmlConverter.setSupportedMediaTypes(mediaTypes)
    val xmlMapper = xmlConverter.getObjectMapper() as XmlMapper
    xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    xmlMapper.enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
    xmlMapper.enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
    xmlMapper.enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
    xmlMapper.getFactory().getXMLOutputFactory()
      .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)

    return xmlConverter
  }

  @Bean
  fun httpPutFormContentFilter(): OrderedFormContentFilter {
    return object : OrderedFormContentFilter() {
      override fun shouldNotFilter(request: HttpServletRequest): Boolean {
        return true
      }
    }
  }

  @Bean
  fun faviconController(): FaviconController {
    return FaviconController()
  }

  @Bean
  fun fileStoreController(objectService: ObjectService, bucketService: BucketService): ObjectController {
    return ObjectController(bucketService, objectService)
  }

  @Bean
  fun bucketController(bucketService: BucketService): BucketController {
    return BucketController(bucketService)
  }

  @Bean
  fun multipartController(
    bucketService: BucketService,
    objectService: ObjectService, multipartService: MultipartService
  ): MultipartController {
    return MultipartController(bucketService, objectService, multipartService)
  }

  @Bean
  fun s3MockExceptionHandler(): S3MockExceptionHandler {
    return S3MockExceptionHandler()
  }

  @Bean
  fun illegalStateExceptionHandler(): IllegalStateExceptionHandler {
    return IllegalStateExceptionHandler()
  }

  @Bean
  fun objectCannedAclHeaderConverter(): ObjectCannedAclHeaderConverter {
    return ObjectCannedAclHeaderConverter()
  }

  /**
   * Spring only provides an ObjectMapper that can serialize but not deserialize XML.
   */
  private fun xmlMapper(): XmlMapper {
    val xmlMapper = XmlMapper.builder()
      .findAndAddModules()
      .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
      .enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
      .enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
      .build()
    xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    xmlMapper.getFactory()
      .getXMLOutputFactory()
      .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)
    return xmlMapper
  }

  @Bean
  fun taggingHeaderConverter(): TaggingHeaderConverter {
    return TaggingHeaderConverter(xmlMapper())
  }

  @Bean
  fun httpRangeHeaderConverter(): HttpRangeHeaderConverter {
    return HttpRangeHeaderConverter()
  }

  @Bean
  fun objectOwnershipHeaderConverter(): ObjectOwnershipHeaderConverter {
    return ObjectOwnershipHeaderConverter()
  }

  @Bean
  fun checksumModeHeaderConverter(): ChecksumModeHeaderConverter {
    return ChecksumModeHeaderConverter()
  }

  @Bean
  fun regionConverter(): RegionConverter {
    return RegionConverter()
  }

  /**
   * [ResponseEntityExceptionHandler] dealing with [S3Exception]s; Serializes them to
   * response output as suitable ErrorResponses.
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
   */
  @ControllerAdvice
  class S3MockExceptionHandler : ResponseEntityExceptionHandler() {
    /**
     * Handles the given [S3Exception].
     *
     * @param s3Exception [S3Exception] to be handled.
     *
     * @return A [ResponseEntity] representing the handled [S3Exception].
     */
    @ExceptionHandler(S3Exception::class)
    fun handleS3Exception(s3Exception: S3Exception): ResponseEntity<ErrorResponse> {
      LOG.debug(
        "Responding with status {}: {}", s3Exception.getStatus(), s3Exception.message,
        s3Exception
      )

      val errorResponse = ErrorResponse(
        s3Exception.getCode(),
        s3Exception.message,
        null,
        null
      )

      val headers = HttpHeaders()
      headers.setContentType(MediaType.APPLICATION_XML)
      if (s3Exception === S3Exception.NO_SUCH_KEY_DELETE_MARKER) {
        headers.set(X_AMZ_DELETE_MARKER, "true")
      }

      return ResponseEntity.status(s3Exception.getStatus()).headers(headers).body<ErrorResponse>(errorResponse)
    }

    companion object {
      private val LOG: Logger = LoggerFactory.getLogger(S3MockExceptionHandler::class.java)
    }
  }

  /**
   * [ResponseEntityExceptionHandler] dealing with [IllegalStateException]s.
   * Serializes them to response output as a 500 Internal Server Error [ErrorResponse].
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
   */
  @ControllerAdvice
  class IllegalStateExceptionHandler : ResponseEntityExceptionHandler() {
    /**
     * Handles the given [IllegalStateException].
     *
     * @param exception [IllegalStateException] to be handled.
     *
     * @return A [ResponseEntity] representing the handled [IllegalStateException].
     */
    @ExceptionHandler(IllegalStateException::class)
    fun handleS3Exception(exception: IllegalStateException): ResponseEntity<ErrorResponse> {
      LOG.debug(
        "Responding with status {}: {}", HttpStatus.INTERNAL_SERVER_ERROR, exception.message,
        exception
      )

      val errorResponse = ErrorResponse(
        "InternalError",
        "We encountered an internal error. Please try again.",
        null,
        null
      )

      val headers = HttpHeaders()
      headers.setContentType(MediaType.APPLICATION_XML)

      return ResponseEntity.internalServerError().headers(headers).body<ErrorResponse>(errorResponse)
    }

    companion object {
      private val LOG: Logger = LoggerFactory.getLogger(IllegalStateExceptionHandler::class.java)
    }
  }
}
