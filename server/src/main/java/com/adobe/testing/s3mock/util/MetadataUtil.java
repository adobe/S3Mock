/*
 *  Copyright 2017-2019 Adobe.
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

import com.adobe.testing.s3mock.domain.S3Object;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

public final class MetadataUtil {

  private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";

  /**
   * Adds user metadata to response headers from S3Object.
   * @param responseHeaders {@link BiConsumer} representing
   * {@link org.springframework.http.HttpHeaders#add(String, String)}.
   * @param s3Object {@link S3Object} S3Object where user metadata will be extracted
   */
  public static void addUserMetadata(final BiConsumer<String, String> responseHeaders,
      final S3Object s3Object) {
    if (s3Object.getUserMetadata() != null) {
      s3Object.getUserMetadata().forEach((key, value) ->
          responseHeaders.accept(HEADER_X_AMZ_META_PREFIX + key, value)
      );
    }
  }

  /**
   * Retrieves user metadata from request.
   * @param request {@link HttpServletRequest}
   * @return map containing user meta-data
   */
  public static Map<String, String> getUserMetadata(final HttpServletRequest request) {
    return Collections.list(request.getHeaderNames()).stream()
        .filter(header -> header.startsWith(HEADER_X_AMZ_META_PREFIX))
        .collect(Collectors.toMap(
            header -> header.substring(HEADER_X_AMZ_META_PREFIX.length()),
            request::getHeader
        ));
  }
}
