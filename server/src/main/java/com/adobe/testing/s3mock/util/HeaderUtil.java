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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.StringEncoding.urlDecode;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;

public final class HeaderUtil {

  private static final String RESPONSE_HEADER_CONTENT_TYPE = "response-content-type";
  private static final String RESPONSE_HEADER_CONTENT_LANGUAGE = "response-content-language";
  private static final String RESPONSE_HEADER_EXPIRES = "response-expires";
  private static final String RESPONSE_HEADER_CACHE_CONTROL = "response-cache-control";
  private static final String RESPONSE_HEADER_CONTENT_DISPOSITION = "response-content-disposition";
  private static final String RESPONSE_HEADER_CONTENT_ENCODING = "response-content-encoding";
  private static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";
  private static final String STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
  private static final MediaType FALLBACK_MEDIA_TYPE = new MediaType("binary", "octet-stream");

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

  /**
   * Creates response headers from S3ObjectMetadata encryption data.
   * @param s3ObjectMetadata {@link S3ObjectMetadata} S3Object where encryption data will be
   *                                                 extracted
   */
  public static Map<String, String> createEncryptionHeaders(S3ObjectMetadata s3ObjectMetadata) {
    Map<String, String> encryptionHeaders = new HashMap<>();
    if (s3ObjectMetadata.isEncrypted()) {
      encryptionHeaders.put(X_AMZ_SERVER_SIDE_ENCRYPTION,
          s3ObjectMetadata.getKmsEncryption());
      encryptionHeaders.put(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          s3ObjectMetadata.getKmsKeyId());
    }
    return encryptionHeaders;
  }

  public static boolean isV4ChunkedWithSigningEnabled(final String sha256Header) {
    return sha256Header != null && sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD);
  }

  public static MediaType parseMediaType(final String contentType) {
    try {
      return MediaType.parseMediaType(contentType);
    } catch (final InvalidMediaTypeException e) {
      return FALLBACK_MEDIA_TYPE;
    }
  }

  public static Map<String, String> createOverrideHeaders(final String query) {
    if (isNotBlank(query)) {
      return Arrays.stream(query.split("&"))
          .filter(param -> isNotBlank(mapHeaderName(
              urlDecode(substringBefore(param, "=")))
          ))
          .collect(Collectors.toMap(
              (param) -> mapHeaderName(urlDecode(substringBefore(param, "="))),
              (param) -> urlDecode(substringAfter(param, "="))));
    }
    return Collections.emptyMap();
  }

  private static String mapHeaderName(final String name) {
    switch (name) {
      case RESPONSE_HEADER_CACHE_CONTROL:
        return HttpHeaders.CACHE_CONTROL;
      case RESPONSE_HEADER_CONTENT_DISPOSITION:
        return HttpHeaders.CONTENT_DISPOSITION;
      case RESPONSE_HEADER_CONTENT_ENCODING:
        return HttpHeaders.CONTENT_ENCODING;
      case RESPONSE_HEADER_CONTENT_LANGUAGE:
        return HttpHeaders.CONTENT_LANGUAGE;
      case RESPONSE_HEADER_CONTENT_TYPE:
        return HttpHeaders.CONTENT_TYPE;
      case RESPONSE_HEADER_EXPIRES:
        return HttpHeaders.EXPIRES;
      default:
        // Only the above header overrides are supported by S3
        return null;
    }
  }
}
