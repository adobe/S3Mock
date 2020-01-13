/*
 *  Copyright 2017-2020 Adobe.
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

import com.adobe.testing.s3mock.domain.S3Exception;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * {@link ResponseEntityExceptionHandler} dealing with {@link S3Exception}s; Serializes them to
 * response output as suitable ErrorResponses. See https://docs.aws.amazon
 * .com/AmazonS3/latest/API/ErrorResponses.html.
 */
@ControllerAdvice
public class S3MockExceptionHandler extends ResponseEntityExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(S3MockExceptionHandler.class);

  /**
   * Handles the given {@link S3Exception}.
   *
   * @param s3Exception {@link S3Exception} to be handled.
   *
   * @return A {@link ResponseEntity} representing the handled {@link S3Exception}.
   */
  @ExceptionHandler
  public ResponseEntity<ErrorResponse> handleS3Exception(final S3Exception s3Exception) {
    LOG.info("Responding with status {}: {}", s3Exception.getStatus(), s3Exception.getMessage());

    final ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode(s3Exception.getCode());
    errorResponse.setMessage(s3Exception.getMessage());

    final HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_XML);

    return ResponseEntity.status(s3Exception.getStatus()).headers(headers).body(errorResponse);
  }
}
