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

package com.adobe.testing.s3mock;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.http.HttpHeaders.HOST;

import com.adobe.testing.s3mock.dto.BucketName;
import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class BucketNameFilterTest {
  private final MockHttpServletResponse response = new MockHttpServletResponse();
  private final FilterChain filterChain = (request, response) -> {
  };
  private MockHttpServletRequest request;

  @Test
  void testGetBucketNameFromPath_awsV1() throws ServletException, IOException {
    request = new MockHttpServletRequest("PUT", "/bucket-name/");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromPath_awsV2() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/bucket-name");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromPath_withKey() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/bucket-name/key-name");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromPath_withContextPath() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/context/bucket-name/key-name");
    BucketNameFilter iut = new BucketNameFilter("context");

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromHost_OK() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/");
    request.addHeader(HOST, "bucket-name.localhost");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromHost_noBucket() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/");
    request.addHeader(HOST, "some-host-name");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNull();
  }

  @Test
  void testGetBucketNameFromHost_withBucketInPath() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/bucket-name/key-name");
    request.addHeader(HOST, "some-host-name");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }

  @Test
  void testGetBucketNameFromIP_noBucket() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/");
    request.addHeader(HOST, "127.0.0.1");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNull();
  }

  @Test
  void testGetBucketNameFromIP_withBucketInPath() throws ServletException, IOException {
    request = new MockHttpServletRequest("GET", "/bucket-name/key-name");
    request.addHeader(HOST, "127.0.0.1");
    BucketNameFilter iut = new BucketNameFilter(null);

    iut.doFilterInternal(request, response, filterChain);

    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isNotNull();
    assertThat(request.getAttribute(BucketNameFilter.BUCKET_ATTRIBUTE)).isEqualTo(
        new BucketName("bucket-name"));
  }
}
