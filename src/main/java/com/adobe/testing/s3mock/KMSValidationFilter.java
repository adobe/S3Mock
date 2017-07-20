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

import static com.adobe.testing.s3mock.util.BetterHeaders.SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.BetterHeaders.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID;

import com.adobe.testing.s3mock.domain.KMSKeyStore;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * A Filter that validates KMS keys of incoming Requests. If Keys can not be found in Keystore
 * the Request will be
 * denied immediately.
 */
class KMSValidationFilter extends OncePerRequestFilter {
  private static final Logger LOG = Logger.getLogger(KMSValidationFilter.class);

  private final KMSKeyStore keystore;

  @Autowired
  private XStreamMarshaller marshaller;

  /**
   * Constructs a new {@link KMSValidationFilter}.
   *
   * @param keystore Keystore for validation of KMS Keys
   *
   */
  public KMSValidationFilter(final KMSKeyStore keystore) {
    this.keystore = keystore;
  }

  @Override
  protected void
  doFilterInternal(final HttpServletRequest request,
      final HttpServletResponse response,
      final FilterChain filterChain) throws ServletException,
      IOException {
    try {
      LOG.debug("Checking KMS key, if present.");
      final String encryptionTypeHeader = request.getHeader(SERVER_SIDE_ENCRYPTION);
      final String encryptionKeyRef = request.getHeader(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID);

      if ("aws:kms".equals(encryptionTypeHeader) && !keystore.validateKeyRef(encryptionKeyRef)) {
        LOG.debug("Received invalid key, sending error response.");

        request.getInputStream().close();

        response.setStatus(HttpStatus.SC_BAD_REQUEST);
        response.setHeader(HttpHeaders.CONTENT_TYPE, "application/xml");

        final ErrorResponse errorResponse = new ErrorResponse();
        errorResponse.setCode("KMS.NotFoundException");
        errorResponse.setMessage("Key " + encryptionKeyRef + " does not exist!");

        marshaller.marshalOutputStream(errorResponse, response.getOutputStream());

        response.flushBuffer();
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
