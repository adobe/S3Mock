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

package com.adobe.testing.s3mock.util;

import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.domain.S3Exception;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

/**
 * {@link HandlerExceptionResolver} dealing with {@link S3Exception}s; Serializes them to response
 * output as suitable ErrorResponses.
 * See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html.
 */
public class S3ExceptionResolver implements HandlerExceptionResolver {
  private static final Logger LOG = LoggerFactory.getLogger(S3ExceptionResolver.class);

  @Autowired
  private XStreamMarshaller marshaller;

  @Override
  public ModelAndView resolveException(final HttpServletRequest request,
      final HttpServletResponse response, final Object o, final Exception e) {
    if (e instanceof S3Exception) {
      final S3Exception s3Exception = (S3Exception) e;
      final ErrorResponse errorResponse = new ErrorResponse();
      errorResponse.setCode(s3Exception.getCode());
      errorResponse.setMessage(s3Exception.getMessage());

      try {
        response.setStatus(s3Exception.getStatus());
        response.setHeader(CONTENT_TYPE, APPLICATION_XML_VALUE);
        marshaller.marshalOutputStream(errorResponse, response.getOutputStream());
        response.flushBuffer();
      } catch (final IOException e1) {
        LOG.info(String.format("Could not write error Response for Exception=%s", e), e1);
      }
    }
    return null;
  }
}
