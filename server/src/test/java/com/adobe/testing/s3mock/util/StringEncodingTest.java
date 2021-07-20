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

import static com.adobe.testing.s3mock.util.StringEncoding.decode;
import static com.adobe.testing.s3mock.util.StringEncoding.encode;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringEncodingTest {

  @Test
  public void testKeyEncoding() {
    assertThat(encode("" + (char) 0x00))
        .as("single char (0x00) encoding")
        .isEqualTo("%00");
    assertThat(encode(":"))
        .as("single char (':') encoding")
        .isEqualTo("%3A");
    assertThat(encode("" + (char) 0x12ab))
        .as("single char (\\u12AB) encoding")
        .isEqualTo("%E1%8A%AB");
    assertThat(encode((char) 0x00 + ":<>%" + (char) 0x7f))
        .as("multiple chars encoding")
        .isEqualTo("%00%3A%3C%3E%25%7F");
    assertThat(encode("foo" + (char) 0x00 + "bar:%baz"))
        .as("mixed encoding")
        .isEqualTo("foo%00bar%3A%25baz");
  }

  @Test
  public void testKeyDecoding() {
    assertThat(decode("%00"))
        .as("single char (0x00) decoding")
        .isEqualTo(("" + (char) 0x00));
    assertThat(decode("%3A"))
        .as("single char (':') decoding")
        .isEqualTo(":");
    assertThat(decode("%E1%8A%AB"))
        .as("single char (\\u12AB) decoding")
        .isEqualTo("" + (char) 0x12ab);
    assertThat(decode("%00%3A%3C%3E%25%7F"))
        .as("multiple chars decoding")
        .isEqualTo((char) 0x00 + ":<>%" + (char) 0x7f);
    assertThat(decode("foo%00bar%3A%25baz"))
        .as("mixed encoding")
        .isEqualTo("foo" + (char) 0x00 + "bar:%baz");
  }
}
