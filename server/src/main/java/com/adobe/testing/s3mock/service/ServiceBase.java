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

package com.adobe.testing.s3mock.service;

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_CONTENT;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DECODED_CONTENT_LENGTH;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk;
import static com.adobe.testing.s3mock.util.HeaderUtil.isChunkedEncoding;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4Signed;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.util.AbstractAwsInputStream;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingChecksumInputStream;
import com.adobe.testing.s3mock.util.AwsUnsignedChunkedDecodingChecksumInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

abstract class ServiceBase {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBase.class);

  public void verifyChecksum(Path path, String checksum, ChecksumAlgorithm checksumAlgorithm) {
    String checksumFor = DigestUtil.checksumFor(path, checksumAlgorithm.toChecksumAlgorithm());
    DigestUtil.verifyChecksum(checksum, checksumFor, checksumAlgorithm);
  }

  public FileChecksum toTempFile(InputStream inputStream, HttpHeaders httpHeaders) {
    try {
      var tempFile = Files.createTempFile("ObjectService", "toTempFile");
      try (var os = Files.newOutputStream(tempFile);
           var wrappedStream = wrapStream(inputStream, httpHeaders)) {
        wrappedStream.transferTo(os);
        ChecksumAlgorithm algorithmFromSdk = checksumAlgorithmFromSdk(httpHeaders);
        if (algorithmFromSdk != null
            && wrappedStream instanceof AbstractAwsInputStream awsInputStream) {
          return new FileChecksum(tempFile, awsInputStream.getChecksum());
        }
        return new FileChecksum(tempFile, null);
      }
    } catch (IOException e) {
      LOG.error("Error reading from InputStream", e);
      throw BAD_REQUEST_CONTENT;
    }
  }

  public FileChecksum toTempFile(InputStream inputStream) {
    try {
      var tempFile = Files.createTempFile("ObjectService", "toTempFile");
      try (var os = Files.newOutputStream(tempFile)) {
        inputStream.transferTo(os);
        return new FileChecksum(tempFile, null);
      }
    } catch (IOException e) {
      LOG.error("Error reading from InputStream", e);
      throw BAD_REQUEST_CONTENT;
    }
  }

  static <T> List<T> mapContents(
      List<T> contents,
      UnaryOperator<T> extractor
  ) {
    return contents
        .stream()
        .map(extractor)
        .toList();
  }

  static <T> List<T> filterBy(
      List<T> contents,
      Function<T, String> function,
      @Nullable String compareTo
  ) {
    if (compareTo != null && !compareTo.isEmpty()) {
      return contents
          .stream()
          .filter(content -> function.apply(content).compareTo(compareTo) > 0)
          .toList();
    } else {
      return contents;
    }
  }

  static <T> List<T> filterBy(
      List<T> contents,
      Function<T, Integer> function,
      @Nullable Integer compareTo
  ) {
    if (compareTo != null) {
      return contents
          .stream()
          .filter(content -> function.apply(content).compareTo(compareTo) > 0)
          .toList();
    } else {
      return contents;
    }
  }

  static <T> List<T> filterBy(
      List<T> contents,
      Function<T, String> function,
      @Nullable List<String> prefixes
  ) {
    if (prefixes != null && !prefixes.isEmpty()) {
      return contents
          .stream()
          .filter(content ->
              prefixes
              .stream()
              .noneMatch(prefix ->
                  function.apply(content).startsWith(prefix)
              )
          )
          .toList();
    } else {
      return contents;
    }
  }

  /**
   * Collapse all elements with keys starting with some prefix up to the given delimiter into
   * one prefix entry. Collapsed elements are removed from the contents list.
   *
   * @param queryPrefix the key prefix as specified in the list request
   * @param delimiter the delimiter used to separate a prefix from the rest of the object name
   * @param contents the list of contents to use for collapsing the prefixes
   * @param function the function to apply on a content to extract the value to collapse the prefixes on
   */
  static <T> List<String> collapseCommonPrefixes(
      @Nullable String queryPrefix,
      @Nullable String delimiter,
      List<T> contents,
      Function<T, String> function
  ) {
    var commonPrefixes = new ArrayList<String>();
    if (delimiter == null || delimiter.isEmpty()) {
      return commonPrefixes;
    }

    var normalizedQueryPrefix = queryPrefix == null ? "" : queryPrefix;

    for (var c : contents) {
      var key = function.apply(c);
      if (key.startsWith(normalizedQueryPrefix)) {
        int delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length());
        if (delimiterIndex > 0) {
          var commonPrefix = key.substring(0, delimiterIndex + delimiter.length());
          if (!commonPrefixes.contains(commonPrefix)) {
            commonPrefixes.add(commonPrefix);
          }
        }
      }
    }
    return commonPrefixes;
  }

  private InputStream wrapStream(InputStream dataStream, HttpHeaders headers) {
    var lengthHeader = headers.getFirst(X_AMZ_DECODED_CONTENT_LENGTH);
    var length = lengthHeader == null ? -1 : Long.parseLong(lengthHeader);
    if (isV4Signed(headers)) {
      return new AwsChunkedDecodingChecksumInputStream(dataStream, length);
    } else if (isChunkedEncoding(headers)) {
      return new AwsUnsignedChunkedDecodingChecksumInputStream(dataStream, length);
    } else {
      return dataStream;
    }
  }
}
