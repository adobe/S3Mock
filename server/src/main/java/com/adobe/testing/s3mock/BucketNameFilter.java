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

import static org.springframework.http.HttpHeaders.HOST;

import com.adobe.testing.s3mock.dto.BucketName;
import com.google.common.net.InetAddresses;
import java.io.IOException;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

class BucketNameFilter extends OncePerRequestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(BucketNameFilter.class);
  private static final Pattern BUCKET_AND_KEY_PATTERN = Pattern.compile("/.+/.*");
  private static final Pattern BUCKET_PATTERN = Pattern.compile("/.+/?");
  static final String BUCKET_ATTRIBUTE = "bucketName";

  private final String contextPath;

  BucketNameFilter(String contextPath) {
    this.contextPath = contextPath;
  }

  @Override
  protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
      FilterChain filterChain) throws ServletException, IOException {
    BucketName bucketName = null;
    try {
      bucketName = fromHost(request);
      if (bucketName == null) {
        bucketName = fromURI(request);
      }
      if (bucketName != null) {
        request.setAttribute(BUCKET_ATTRIBUTE, bucketName);
      }
    } finally {
      LOG.info("Found bucketName {}", bucketName);
      filterChain.doFilter(request, response);
    }
  }

  private BucketName fromURI(HttpServletRequest request) {
    String requestURI = request.getRequestURI();
    LOG.info("Check for bucket name in request URI={}.", requestURI);
    if (BUCKET_AND_KEY_PATTERN.matcher(requestURI).matches()
        || BUCKET_PATTERN.matcher(requestURI).matches()) {
      String bucketName = fromURIString(requestURI);
      return new BucketName(bucketName);
    }

    return null;
  }

  private String fromURIString(String uri) {
    String bucketName = null;
    String[] uriComponents = uri.split("/");
    if (uriComponents.length > 1) {
      String firstElement = uriComponents[1];
      if (firstElement.equals(contextPath) && uriComponents.length > 2) {
        bucketName = uriComponents[2];
      } else {
        bucketName = firstElement;
      }
    }

    return bucketName;
  }

  private BucketName fromHost(HttpServletRequest request) {
    String host = request.getHeader(HOST);
    LOG.info("Check for bucket name in host={}.", host);
    if (host == null || InetAddresses.isUriInetAddress(host)) {
      return null;
    }

    String bucketName = getBucketName(host);
    if (bucketName != null) {
      return new BucketName(bucketName);
    }
    return null;
  }

  private String getBucketName(String hostName) {
    if (hostName.contains(".")) {
      String[] hostNameComponents = hostName.split("\\.");
      return hostNameComponents[0];
    }
    return null;
  }
}
