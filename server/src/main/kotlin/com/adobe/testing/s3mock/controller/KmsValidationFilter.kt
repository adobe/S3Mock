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

import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID
import jakarta.servlet.FilterChain
import jakarta.servlet.ServletException
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter
import org.springframework.web.filter.OncePerRequestFilter
import java.io.IOException

/**
 * A Filter that validates KMS keys of incoming Requests. If Keys can not be found in Keystore the
 * Request will be denied immediately.
 */
internal class KmsValidationFilter
/**
 * Constructs a new [KmsValidationFilter].
 *
 * @param keystore Keystore for validation of KMS Keys
 */(
  private val keystore: KmsKeyStore,
  private val messageConverter: MappingJackson2XmlHttpMessageConverter
) : OncePerRequestFilter() {
  @Throws(ServletException::class, IOException::class)
  override fun doFilterInternal(
    request: HttpServletRequest,
    response: HttpServletResponse,
    filterChain: FilterChain
  ) {
    LOG.debug("Checking KMS key, if present.")
    try {
      val encryptionType = request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)
      val keyId = request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID)

      val isAwsKms = encryptionType == AWS_KMS
      val hasKeyId = !keyId.isNullOrBlank()

      if (isAwsKms && hasKeyId && !keystore.validateKeyId(keyId)) {
        LOG.info("Received invalid KMS key ID {}. Sending error response.", keyId)

        runCatching { request.inputStream.close() }

        response.status = HttpStatus.BAD_REQUEST.value()
        response.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_XML_VALUE)

        val error = ErrorResponse(
          "KMS.NotFoundException",
          "Invalid keyId '$keyId'",
          null,
          null
        )
        messageConverter.objectMapper.writeValue(response.outputStream, error)
        response.flushBuffer()
        return
      }

      if (isAwsKms && hasKeyId) {
        LOG.info("Received valid KMS key ID {}.", keyId)
      }

      filterChain.doFilter(request, response)
    } finally {
      LOG.debug("Finished checking KMS key.")
    }
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(KmsValidationFilter::class.java)
    private const val AWS_KMS = "aws:kms"
  }
}
