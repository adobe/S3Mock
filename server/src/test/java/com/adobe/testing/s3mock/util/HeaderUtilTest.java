/*
 *  Copyright 2017-2023 Adobe.
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

import static com.adobe.testing.s3mock.util.HeaderUtil.createUserMetadataHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseUserMetadata;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

class HeaderUtilTest {

  private static final String X_AMZ_CANONICAL_HEADER = "X-Amz-Meta-Some-header";
  private static final String X_AMZ_LOWERCASE_HEADER = "x-amz-meta-Some-header";
  private static final String TEST_VALUE = "test-value";

  @Test
  void testGetUserMetadata_canonical() {
    var httpHeaders = new HttpHeaders();
    httpHeaders.add(X_AMZ_CANONICAL_HEADER, TEST_VALUE);

    var userMetadata = parseUserMetadata(httpHeaders);
    assertThat(userMetadata).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
  }

  @Test
  void testGetUserMetadata_javaSdk() {
    var httpHeaders = new HttpHeaders();
    httpHeaders.add(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);

    var userMetadata = parseUserMetadata(httpHeaders);
    assertThat(userMetadata).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
  }

  @Test
  void testCreateUserMetadata_canonical() {
    var userMetadata = Map.of(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
    var s3ObjectMetadata = createObjectMetadata(userMetadata);

    var userMetadataHeaders = createUserMetadataHeaders(s3ObjectMetadata);
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
  }

  @Test
  void testCreateUserMetadata_javaSdk() {
    var userMetadata = Map.of(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
    var s3ObjectMetadata = createObjectMetadata(userMetadata);

    var userMetadataHeaders = createUserMetadataHeaders(s3ObjectMetadata);
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
  }

  S3ObjectMetadata createObjectMetadata(Map<String, String> userMetadata) {
    return new S3ObjectMetadata(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        0L,
        null,
        null,
        userMetadata,
        null,
        null,
        null,
        null
    );
  }
}
