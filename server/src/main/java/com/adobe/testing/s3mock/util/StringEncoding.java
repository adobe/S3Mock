/*
 *  Copyright 2017-2021 Adobe.
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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for {@link URLEncoder#encode(String, String)} and
 *   {@link URLDecoder#decode(String, String)} since those methods throw exceptions.
 */
public class StringEncoding {

  private static final Logger LOG = LoggerFactory.getLogger(StringEncoding.class);

  public static String encode(String toEncode) {
    try {
      return URLEncoder.encode(toEncode, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding string {}", toEncode, e);
      throw new AssertionError(UTF_8.name() + " is unknown");
    }
  }

  public static String decode(String toDecode) {
    try {
      return URLDecoder.decode(toDecode, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error decoding string {}", toDecode, e);
      throw new AssertionError(UTF_8.name() + " is unknown");
    }
  }
}
