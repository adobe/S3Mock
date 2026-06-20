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
package com.adobe.testing.s3mock.vectors.service

import com.adobe.testing.s3mock.vectors.S3VectorsException
import java.util.Base64

open class VectorServiceBase {
  fun encodeToken(value: String): String =
    Base64
      .getEncoder()
      .encodeToString(value.toByteArray())

  fun decodeToken(token: String): String =
    try {
      String(Base64.getDecoder().decode(token))
    } catch (_: IllegalArgumentException) {
      throw S3VectorsException.validation("Invalid nextToken.")
    }
}
