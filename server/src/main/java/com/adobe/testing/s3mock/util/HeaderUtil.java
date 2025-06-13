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

package com.adobe.testing.s3mock.util;

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.AWS_CHUNKED;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC32C;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_CRC64NVME;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA1;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;

public final class HeaderUtil {

  public static final String HEADER_X_AMZ_META_PREFIX = "x-amz-meta-";
  private static final String RESPONSE_HEADER_CONTENT_TYPE = "response-content-type";
  private static final String RESPONSE_HEADER_CONTENT_LANGUAGE = "response-content-language";
  private static final String RESPONSE_HEADER_EXPIRES = "response-expires";
  private static final String RESPONSE_HEADER_CACHE_CONTROL = "response-cache-control";
  private static final String RESPONSE_HEADER_CONTENT_DISPOSITION = "response-content-disposition";
  private static final String RESPONSE_HEADER_CONTENT_ENCODING = "response-content-encoding";
  private static final String STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
  private static final String STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD_TRAILER =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
  private static final MediaType FALLBACK_MEDIA_TYPE = MediaType.APPLICATION_OCTET_STREAM;

  private HeaderUtil() {
    // private constructor for utility classes
  }

  /**
   * Creates response headers from S3ObjectMetadata user metadata.
   *
   * @param s3ObjectMetadata {@link S3ObjectMetadata} S3Object where user metadata will be extracted
   */
  public static Map<String, String> userMetadataHeadersFrom(S3ObjectMetadata s3ObjectMetadata) {
    Map<String, String> metadataHeaders = new HashMap<>();
    if (s3ObjectMetadata.userMetadata() != null) {
      s3ObjectMetadata.userMetadata()
              .forEach((key, value) -> {
                if (key.regionMatches(true, 0, HEADER_X_AMZ_META_PREFIX, 0, HEADER_X_AMZ_META_PREFIX.length())) {
                  metadataHeaders.put(key, value);
                } else {
                  // support case where metadata was stored locally in legacy format
                  metadataHeaders.put(HEADER_X_AMZ_META_PREFIX + key, value);
                }
              });
    }
    return metadataHeaders;
  }

  /**
   * Creates response headers from S3ObjectMetadata storageclass.
   *
   * @param s3ObjectMetadata {@link S3ObjectMetadata} S3Object where data will be extracted
   */
  public static Map<String, String> storageClassHeadersFrom(S3ObjectMetadata s3ObjectMetadata) {
    Map<String, String> headers = new HashMap<>();
    StorageClass storageClass = s3ObjectMetadata.storageClass();
    if (storageClass != null) {
      headers.put(X_AMZ_STORAGE_CLASS, storageClass.toString());
    }
    return headers;
  }

  /**
   * Retrieves user metadata from request.
   *
   * @param headers {@link HttpHeaders}
   * @return map containing user meta-data
   */
  public static Map<String, String> userMetadataFrom(HttpHeaders headers) {
    return parseHeadersToMap(headers,
        header -> header.regionMatches(true, 0, HEADER_X_AMZ_META_PREFIX, 0, HEADER_X_AMZ_META_PREFIX.length()));
  }

  /**
   * Retrieves headers to store from request.
   *
   * @param headers {@link HttpHeaders}
   * @return map containing headers to store
   */
  public static Map<String, String> storeHeadersFrom(HttpHeaders headers) {
    return parseHeadersToMap(headers,
        header -> (HttpHeaders.EXPIRES.equalsIgnoreCase(header)
            || HttpHeaders.CONTENT_LANGUAGE.equalsIgnoreCase(header)
            || HttpHeaders.CONTENT_DISPOSITION.equalsIgnoreCase(header)
            || (HttpHeaders.CONTENT_ENCODING.equalsIgnoreCase(header)
                && !isOnlyChunkedEncoding(headers))
            || HttpHeaders.CACHE_CONTROL.equalsIgnoreCase(header)
        ));
  }

  /**
   * Retrieves headers encryption headers from request.
   *
   * @param headers {@link HttpHeaders}
   * @return map containing encryption headers
   */
  public static Map<String, String> encryptionHeadersFrom(HttpHeaders headers) {
    return parseHeadersToMap(headers,
        header ->
            header.regionMatches(true, 0, X_AMZ_SERVER_SIDE_ENCRYPTION, 0, X_AMZ_SERVER_SIDE_ENCRYPTION.length()));
  }

  private static Map<String, String> parseHeadersToMap(HttpHeaders headers,
      Predicate<String> matcher) {
    return headers
        .headerSet()
        .stream()
        .map(
            entry -> {
              if (matcher.test(entry.getKey())
                  && entry.getValue() != null
                  && !entry.getValue().isEmpty()) {
                String value = entry.getValue().getFirst();
                if (value != null && !value.isBlank()) {
                  return new SimpleEntry<>(entry.getKey(), entry.getValue().getFirst());
                } else {
                  return null;
                }
              } else {
                return null;
              }
            }
        )
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }

  public static boolean isV4Signed(HttpHeaders headers) {
    var sha256Header = headers.getFirst(X_AMZ_CONTENT_SHA256);
    return sha256Header != null
        && (sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD)
            || sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD_TRAILER));
  }

  public static boolean isChunkedEncoding(HttpHeaders headers) {
    var contentEncodingHeaders = headers.get(HttpHeaders.CONTENT_ENCODING);
    return (contentEncodingHeaders != null
        && (contentEncodingHeaders.contains(AWS_CHUNKED)));
  }

  /**
   * Check if aws-chunked is the only "Content-Encoding" header.
   * <quote>
   *   If aws-chunked is the only value that you pass in the content-encoding header, S3 considers
   *   the content-encoding header empty and does not return this header when your retrieve the
   *   object.
   * </quote>
   * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html">API</a>
   */
  private static boolean isOnlyChunkedEncoding(HttpHeaders headers) {
    var contentEncodingHeaders = headers.get(HttpHeaders.CONTENT_ENCODING);
    return (contentEncodingHeaders != null
        && contentEncodingHeaders.size() == 1
        && (contentEncodingHeaders.contains(AWS_CHUNKED)));
  }

  public static MediaType mediaTypeFrom(@Nullable String contentType) {
    try {
      return MediaType.parseMediaType(contentType);
    } catch (final InvalidMediaTypeException e) {
      return FALLBACK_MEDIA_TYPE;
    }
  }

  public static Map<String, String> overrideHeadersFrom(Map<String, String> queryParams) {
    return queryParams
        .entrySet()
        .stream()
        .map(
            entry -> {
              String mapHeaderName = mapHeaderName(entry.getKey());
              if (!mapHeaderName.isBlank()) {
                return new SimpleEntry<>(mapHeaderName, entry.getValue());
              } else {
                return null;
              }
            }
        )
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }


  public static Map<String, String> checksumHeaderFrom(S3ObjectMetadata s3ObjectMetadata) {
    ChecksumAlgorithm checksumAlgorithm = s3ObjectMetadata.checksumAlgorithm();
    String checksum = s3ObjectMetadata.checksum();
    return checksumHeaderFrom(checksum, checksumAlgorithm);
  }

  public static Map<String, String> checksumHeaderFrom(
      @Nullable String checksum,
      @Nullable ChecksumAlgorithm checksumAlgorithm) {
    Map<String, String> headers = new HashMap<>();
    if (checksumAlgorithm != null && checksum != null) {
      headers.put(mapChecksumToHeader(checksumAlgorithm), checksum);
    }
    return headers;
  }

  @Nullable
  public static ChecksumAlgorithm checksumAlgorithmFromHeader(HttpHeaders headers) {
    if (headers.containsHeader(X_AMZ_CHECKSUM_SHA256)) {
      return ChecksumAlgorithm.SHA256;
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_SHA1)) {
      return ChecksumAlgorithm.SHA1;
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32)) {
      return ChecksumAlgorithm.CRC32;
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32C)) {
      return ChecksumAlgorithm.CRC32C;
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME)) {
      return ChecksumAlgorithm.CRC64NVME;
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_ALGORITHM)) {
      var checksumAlgorithm = headers.getFirst(X_AMZ_CHECKSUM_ALGORITHM);
      return ChecksumAlgorithm.fromString(checksumAlgorithm);
    } else {
      return null;
    }
  }

  @Nullable
  public static ChecksumAlgorithm checksumAlgorithmFromSdk(HttpHeaders headers) {
    if (headers.containsHeader(X_AMZ_SDK_CHECKSUM_ALGORITHM)) {
      return ChecksumAlgorithm.fromString(headers.getFirst(X_AMZ_SDK_CHECKSUM_ALGORITHM));
    } else {
      return null;
    }
  }

  @Nullable
  public static String checksumFrom(HttpHeaders headers) {
    if (headers.containsHeader(X_AMZ_CHECKSUM_SHA256)) {
      return headers.getFirst(X_AMZ_CHECKSUM_SHA256);
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_SHA1)) {
      return headers.getFirst(X_AMZ_CHECKSUM_SHA1);
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32)) {
      return headers.getFirst(X_AMZ_CHECKSUM_CRC32);
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC32C)) {
      return headers.getFirst(X_AMZ_CHECKSUM_CRC32C);
    } else if (headers.containsHeader(X_AMZ_CHECKSUM_CRC64NVME)) {
      return headers.getFirst(X_AMZ_CHECKSUM_CRC64NVME);
    }
    return null;
  }

  public static String mapChecksumToHeader(ChecksumAlgorithm checksumAlgorithm) {
    return switch (checksumAlgorithm) {
      case SHA256 -> X_AMZ_CHECKSUM_SHA256;
      case SHA1 -> X_AMZ_CHECKSUM_SHA1;
      case CRC32 -> X_AMZ_CHECKSUM_CRC32;
      case CRC32C -> X_AMZ_CHECKSUM_CRC32C;
      case CRC64NVME -> X_AMZ_CHECKSUM_CRC64NVME;
    };
  }

  private static String mapHeaderName(final String name) {
    return switch (name) {
      case RESPONSE_HEADER_CACHE_CONTROL -> HttpHeaders.CACHE_CONTROL;
      case RESPONSE_HEADER_CONTENT_DISPOSITION -> HttpHeaders.CONTENT_DISPOSITION;
      case RESPONSE_HEADER_CONTENT_ENCODING -> HttpHeaders.CONTENT_ENCODING;
      case RESPONSE_HEADER_CONTENT_LANGUAGE -> HttpHeaders.CONTENT_LANGUAGE;
      case RESPONSE_HEADER_CONTENT_TYPE -> HttpHeaders.CONTENT_TYPE;
      case RESPONSE_HEADER_EXPIRES -> HttpHeaders.EXPIRES;
      // Only the above header overrides are supported by S3
      default -> "";
    };
  }
}
