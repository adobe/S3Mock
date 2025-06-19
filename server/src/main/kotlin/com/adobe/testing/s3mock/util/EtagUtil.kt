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

package com.adobe.testing.s3mock.util

object EtagUtil {
  /**
   * Returns etag in normalized form with surrounding quotes.
   * This normalized form is persisted so that S3Mock can conform to RFC2616 / RFC7232.
   * [RFC2616](https://www.rfc-editor.org/rfc/rfc2616#section-14.19)
   * [RFC7232](https://www.rfc-editor.org/rfc/rfc7232)
   */
  @JvmStatic
  fun normalizeEtag(etag: String?): String? {
    return if (etag == null) {
      null
    } else if (etag.startsWith("\"") && etag.endsWith("\"")) {
      etag
    } else {
      "\"$etag\""
    }
  }
}
