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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * A Filter that validates KMS keys of incoming Requests. If Keys can not be found in Keystore the
 * Request will be denied immediately.
 */
class KmsValidationFilter extends OncePerRequestFilter {

  private static final Logger LOG = LoggerFactory.getLogger(KmsValidationFilter.class);

  private static final String AWS_KMS = "aws:kms";

  private final KmsKeyStore keystore;

  private final MappingJackson2XmlHttpMessageConverter messageConverter;

  /**
   * Constructs a new {@link KmsValidationFilter}.
   *
   * @param keystore Keystore for validation of KMS Keys
   */
  KmsValidationFilter(KmsKeyStore keystore,
      MappingJackson2XmlHttpMessageConverter messageConverter) {
    this.keystore = keystore;
    this.messageConverter = messageConverter;
  }

  @Override
  protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
      FilterChain filterChain) throws ServletException, IOException {
    try {
      LOG.debug("Checking KMS key, if present.");
      final String encryptionTypeHeader = request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION);
      final String encryptionKeyId =
          request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);

      if (AWS_KMS.equals(encryptionTypeHeader)
          && !isBlank(encryptionKeyId)
          && !keystore.validateKeyId(encryptionKeyId)) {
        LOG.info("Received invalid KMS key ID {}. Sending error response.", encryptionKeyId);

        request.getInputStream().close();

        response.setStatus(BAD_REQUEST.value());
        response.setHeader(CONTENT_TYPE, APPLICATION_XML_VALUE);

        final ErrorResponse errorResponse = new ErrorResponse(
            "KMS.NotFoundException",
            "Key ID " + encryptionKeyId + " does not exist!",
            null,
            null
        );

        messageConverter.getObjectMapper().writeValue(response.getOutputStream(), errorResponse);

        response.flushBuffer();
      } else if (AWS_KMS.equals(encryptionTypeHeader)
          && !isBlank(encryptionKeyId)
          && keystore.validateKeyId(encryptionKeyId)) {
        LOG.info("Received valid KMS key ID {}.", encryptionKeyId);
        filterChain.doFilter(request, response);
      } else {
        filterChain.doFilter(request, response);
      }
    } catch (final RuntimeException e) {
      LOG.error("Caught exception", e);
      throw e;
    } finally {
      LOG.debug("Finished checking KMS key.");
    }
  }
}
