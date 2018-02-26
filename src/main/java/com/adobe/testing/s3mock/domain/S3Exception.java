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

package com.adobe.testing.s3mock.domain;

/**
 * {@link RuntimeException} to communicate general S3 errors. These are handled by
 * {@code com.adobe.testing.s3mock.util.S3ExceptionResolver}, mapped to
 * {@link com.adobe.testing.s3mock.dto.ErrorResponse} and serialized.
 */
public class S3Exception extends RuntimeException {
  private final int status;
  private final String code;
  private final String message;

  public S3Exception(final int status, final String code, final String message) {
    super(message);
    this.status = status;
    this.code = code;
    this.message = message;
  }

  public int getStatus() {
    return status;
  }

  public String getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    return message;
  }
}
