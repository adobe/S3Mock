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
import static com.adobe.testing.s3mock.util.HeaderUtil.getUserMetadata;
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
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.add(X_AMZ_CANONICAL_HEADER, TEST_VALUE);

    Map<String, String> userMetadata = getUserMetadata(httpHeaders);
    assertThat(userMetadata).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
  }

  @Test
  void testGetUserMetadata_javaSdk() {
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.add(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);

    Map<String, String> userMetadata = getUserMetadata(httpHeaders);
    assertThat(userMetadata).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
  }

  @Test
  void testCreateUserMetadata_canonical() {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
    s3ObjectMetadata.setUserMetadata(userMetadata);

    Map<String, String> userMetadataHeaders = createUserMetadataHeaders(s3ObjectMetadata);
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_CANONICAL_HEADER, TEST_VALUE);
  }

  @Test
  void testCreateUserMetadata_javaSdk() {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
    s3ObjectMetadata.setUserMetadata(userMetadata);

    Map<String, String> userMetadataHeaders = createUserMetadataHeaders(s3ObjectMetadata);
    assertThat(userMetadataHeaders).containsEntry(X_AMZ_LOWERCASE_HEADER, TEST_VALUE);
  }
}
