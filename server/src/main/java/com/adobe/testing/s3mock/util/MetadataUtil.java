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

package com.adobe.testing.s3mock.util;

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

public final class MetadataUtil {

  private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";

  /**
   * Creates response headers from S3ObjectMetadata user metadata.
   * @param s3ObjectMetadata {@link S3ObjectMetadata} S3Object where user metadata will be extracted
   */
  public static Map<String, String> createUserMetadataHeaders(S3ObjectMetadata s3ObjectMetadata) {
    Map<String, String> metadataHeaders = new HashMap<>();
    if (s3ObjectMetadata.getUserMetadata() != null) {
      s3ObjectMetadata.getUserMetadata().forEach((key, value) ->
          metadataHeaders.put(HEADER_X_AMZ_META_PREFIX + key, value)
      );
    }
    return metadataHeaders;
  }

  /**
   * Retrieves user metadata from request.
   * @param request {@link HttpServletRequest}
   * @return map containing user meta-data
   */
  public static Map<String, String> getUserMetadata(HttpServletRequest request) {
    return Collections.list(request.getHeaderNames()).stream()
        .filter(header -> header.startsWith(HEADER_X_AMZ_META_PREFIX))
        .collect(Collectors.toMap(
            header -> header.substring(HEADER_X_AMZ_META_PREFIX.length()),
            request::getHeader
        ));
  }
}
