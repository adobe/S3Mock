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
package com.adobe.testing.s3mock.vectors.controller

import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZN_ERROR_TYPE
import com.adobe.testing.s3mock.vectors.S3VectorsException
import com.adobe.testing.s3mock.vectors.dto.VectorsErrorResponse
import com.adobe.testing.s3mock.vectors.service.VectorBucketService
import com.adobe.testing.s3mock.vectors.service.VectorIndexService
import com.adobe.testing.s3mock.vectors.service.VectorQueryService
import com.adobe.testing.s3mock.vectors.service.VectorService
import com.fasterxml.jackson.annotation.JsonInclude
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.Ordered.LOWEST_PRECEDENCE
import org.springframework.core.annotation.Order
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageConverter
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

@Configuration
@Profile("vectors")
@Order(LOWEST_PRECEDENCE)
class VectorsControllerConfiguration : WebMvcConfigurer {
  /**
   * Registers a [JacksonJsonHttpMessageConverter] backed by an explicit Kotlin-aware
   * [JsonMapper]. Spring Boot picks up `HttpMessageConverter` beans registered this way.
   * This converter is placed at position 0 (highest priority) via [extendMessageConverters],
   * ensuring S3 Vectors request/response bodies are serialized as JSON using the Kotlin module.
   */
  @Bean
  fun vectorsJsonHttpMessageConverter(): JacksonJsonHttpMessageConverter =
    JacksonJsonHttpMessageConverter(
      JsonMapper
        .builder()
        .addModule(KotlinModule.Builder().build())
        .findAndAddModules()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .changeDefaultPropertyInclusion { it.withValueInclusion(JsonInclude.Include.NON_NULL) }
        .build(),
    )

  // Appends application/json to the fixed default content types so that requests with
  // Accept: any-wildcard (which is what the AWS S3 Vectors SDK sends) can match vectors
  // endpoints that declare produces = [APPLICATION_JSON_VALUE].
  // APPLICATION_XML is kept before APPLICATION_JSON so that S3 (XML) endpoints still
  // win for wildcard Accept requests — they only produce XML, so the first match in the
  // list is APPLICATION_XML.
  override fun configureContentNegotiation(configurer: ContentNegotiationConfigurer) {
    configurer.defaultContentType(
      MediaType.APPLICATION_FORM_URLENCODED,
      MediaType.APPLICATION_XML,
      MediaType.APPLICATION_JSON,
    )
  }

  override fun extendMessageConverters(converters: MutableList<HttpMessageConverter<*>>) {
    converters.add(0, vectorsJsonHttpMessageConverter())
  }

  @Bean
  fun vectorBucketController(vectorBucketService: VectorBucketService): VectorBucketController = VectorBucketController(vectorBucketService)

  @Bean
  fun vectorIndexController(vectorIndexService: VectorIndexService): VectorIndexController = VectorIndexController(vectorIndexService)

  @Bean
  fun vectorController(vectorService: VectorService): VectorController = VectorController(vectorService)

  @Bean
  fun vectorQueryController(vectorQueryService: VectorQueryService): VectorQueryController = VectorQueryController(vectorQueryService)

  @Bean
  fun vectorPolicyController(vectorBucketService: VectorBucketService): VectorPolicyController = VectorPolicyController(vectorBucketService)

  @Bean
  fun vectorTaggingController(
    vectorBucketService: VectorBucketService,
    vectorIndexService: VectorIndexService,
  ): VectorTaggingController = VectorTaggingController(vectorBucketService, vectorIndexService)

  @Bean
  fun s3VectorsExceptionHandler(): S3VectorsExceptionHandler = S3VectorsExceptionHandler()

  @Bean
  fun illegalStateVectorsExceptionHandler(): IllegalStateVectorsExceptionHandler = IllegalStateVectorsExceptionHandler()

  /**
   * Handles [S3VectorsException]s and serializes them to JSON with `__type` and `message` fields,
   * matching the AWS S3 Vectors error wire format.
   */
  @RestControllerAdvice(
    basePackageClasses = [VectorBucketController::class],
  )
  class S3VectorsExceptionHandler : ResponseEntityExceptionHandler() {
    @ExceptionHandler(S3VectorsException::class)
    fun handleS3VectorsException(ex: S3VectorsException): ResponseEntity<VectorsErrorResponse> {
      LOG.debug("Responding with status {}: {}", ex.status, ex.message, ex)
      val headers =
        HttpHeaders().apply {
          contentType = MediaType.APPLICATION_JSON
          // X-Amzn-Errortype is the primary signal the AWS SDK v2 uses to map JSON errors to
          // typed exception classes (ConflictException, NotFoundException, etc.). The __type
          // field in the body is a fallback; setting the header ensures reliable SDK mapping.
          set(X_AMZN_ERROR_TYPE, ex.type)
        }
      return ResponseEntity
        .status(ex.status)
        .headers(headers)
        .body(VectorsErrorResponse("com.amazonaws.s3vectors#${ex.type}", ex.message))
    }

    companion object {
      private val LOG: Logger = LoggerFactory.getLogger(S3VectorsExceptionHandler::class.java)
    }
  }

  @RestControllerAdvice(
    basePackageClasses = [VectorBucketController::class],
  )
  class IllegalStateVectorsExceptionHandler : ResponseEntityExceptionHandler() {
    @ExceptionHandler(IllegalStateException::class)
    fun handleIllegalState(ex: IllegalStateException): ResponseEntity<VectorsErrorResponse> {
      LOG.debug("Responding with 500: {}", ex.message, ex)
      val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
      return ResponseEntity
        .status(HttpStatus.INTERNAL_SERVER_ERROR)
        .headers(headers)
        .body(VectorsErrorResponse("com.amazonaws.s3vectors#InternalError", "We encountered an internal error. Please try again."))
    }

    companion object {
      private val LOG: Logger = LoggerFactory.getLogger(IllegalStateVectorsExceptionHandler::class.java)
    }
  }
}
