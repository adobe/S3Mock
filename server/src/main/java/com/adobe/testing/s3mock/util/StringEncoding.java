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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.replaceEach;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains verbatim copies of encode/decode methods from
 * "software.amazon.awssdk.utils.http.SdkHttpUtils" since we need to exactly emulate the AWS SDK
 * expectation for some values.
 */
public class StringEncoding {

  private static final Logger LOG = LoggerFactory.getLogger(StringEncoding.class);
  /**
   * Characters that we need to fix up after URLEncoder.encode().
   */
  private static final String[] ENCODED_CHARACTERS_WITH_SLASHES =
      new String[] {"+", "*", "%7E", "%2F"};
  private static final String[] ENCODED_CHARACTERS_WITH_SLASHES_REPLACEMENTS =
      new String[] {"%20", "%2A", "~", "/"};
  private static final String[] ENCODED_CHARACTERS_WITHOUT_SLASHES =
      new String[] {"+", "*", "%7E"};
  private static final String[] ENCODED_CHARACTERS_WITHOUT_SLASHES_REPLACEMENTS =
      new String[] {"%20", "%2A", "~"};

  /**
   * Decode the string according to RFC 3986: encoding for URI paths, query strings, etc.
   * Assumes the decoded string is UTF-8 encoded.
   *
   * @param value The string to decode.
   * @return The decoded string.
   */
  public static String urlDecode(String value) {
    if (value == null) {
      return null;
    }
    try {
      return URLDecoder.decode(value, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error decoding string {}", value, e);
      throw new RuntimeException("Unable to decode value", e);
    }
  }

  /**
   * Encode a string according to RFC 3986, but ignore "/" characters.
   * This is useful for encoding the components of a path, without encoding the path separators.
   */
  public static String urlEncodeIgnoreSlashes(String value) {
    return urlEncode(value, true);
  }

  /**
   * Encode a string according to RFC 3986: encoding for URI paths, query strings, etc.
   */
  public static String urlEncode(String value) {
    return urlEncode(value, false);
  }

  /**
   * Encode a string for use in the path of a URL; uses URLEncoder.encode,
   * (which encodes a string for use in the query portion of a URL), then
   * applies some postfilters to fix things up per the RFC. Can optionally
   * handle strings which are meant to encode a path (ie include '/'es
   * which should NOT be escaped).
   *
   * @param value the value to encode
   * @param ignoreSlashes  true if the value is intended to represent a path
   * @return the encoded value
   */
  private static String urlEncode(String value, boolean ignoreSlashes) {
    if (value == null) {
      return null;
    }
    String encoded;
    try {
      encoded = URLEncoder.encode(value, UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error encoding string {}", value, e);
      throw new RuntimeException("Unable to encode value", e);
    }

    if (!ignoreSlashes) {
      return replaceEach(encoded,
          ENCODED_CHARACTERS_WITHOUT_SLASHES,
          ENCODED_CHARACTERS_WITHOUT_SLASHES_REPLACEMENTS);
    }

    return replaceEach(encoded,
        ENCODED_CHARACTERS_WITH_SLASHES,
        ENCODED_CHARACTERS_WITH_SLASHES_REPLACEMENTS);
  }
}
