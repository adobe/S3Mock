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

import static com.adobe.testing.s3mock.util.StringEncoding.urlDecode;
import static com.adobe.testing.s3mock.util.StringEncoding.urlEncode;
import static com.adobe.testing.s3mock.util.StringEncoding.urlEncodeIgnoreSlashes;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringEncodingTest {

  @Test
  public void testKeyEncoding() {
    assertThat(urlEncode("" + (char) 0x00))
        .as("single char (0x00) encoding")
        .isEqualTo("%00");
    assertThat(urlEncode(":"))
        .as("single char (':') encoding")
        .isEqualTo("%3A");
    assertThat(urlEncode("" + (char) 0x12ab))
        .as("single char (\\u12AB) encoding")
        .isEqualTo("%E1%8A%AB");
    assertThat(urlEncode((char) 0x00 + ":<>%" + (char) 0x7f))
        .as("multiple chars encoding")
        .isEqualTo("%00%3A%3C%3E%25%7F");
    assertThat(urlEncode("foo" + (char) 0x00 + "bar:%baz"))
        .as("mixed encoding")
        .isEqualTo("foo%00bar%3A%25baz");
  }

  @Test
  public void testKeyDecoding() {
    assertThat(urlDecode("%00"))
        .as("single char (0x00) decoding")
        .isEqualTo(("" + (char) 0x00));
    assertThat(urlDecode("%3A"))
        .as("single char (':') decoding")
        .isEqualTo(":");
    assertThat(urlDecode("%E1%8A%AB"))
        .as("single char (\\u12AB) decoding")
        .isEqualTo("" + (char) 0x12ab);
    assertThat(urlDecode("%00%3A%3C%3E%25%7F"))
        .as("multiple chars decoding")
        .isEqualTo((char) 0x00 + ":<>%" + (char) 0x7f);
    assertThat(urlDecode("foo%00bar%3A%25baz"))
        .as("mixed encoding")
        .isEqualTo("foo" + (char) 0x00 + "bar:%baz");
  }

  @Test
  public void urlValuesEncodeCorrectly() {
    String nonEncodedCharacters =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~";
    String encodedCharactersInput =
        "\t\n\r !\"#$%&'()*+,/:;<=>?@[\\]^`{|}";
    String encodedCharactersOutput =
        "%09%0A%0D%20%21%22%23%24%25%26%27%28%29%2A%2B%2C%2F%3A%3B%3C%3D%3E%3F%40%5B%5C%5D%5E%60%7B"
            + "%7C%7D";

    assertThat(urlEncode(null)).isEqualTo(null);
    assertThat(urlEncode("")).isEqualTo("");
    assertThat(urlEncode(nonEncodedCharacters)).isEqualTo(nonEncodedCharacters);
    assertThat(urlEncode(encodedCharactersInput)).isEqualTo(encodedCharactersOutput);
  }

  @Test
  public void encodeUrlIgnoreSlashesEncodesCorrectly() {
    String nonEncodedCharacters =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~/";
    String encodedCharactersInput =
        "\t\n\r !\"#$%&'()*+,:;<=>?@[\\]^`{|}";
    String encodedCharactersOutput =
        "%09%0A%0D%20%21%22%23%24%25%26%27%28%29%2A%2B%2C%3A%3B%3C%3D%3E%3F%40%5B%5C%5D%5E%60%7B"
            + "%7C%7D";

    assertThat(urlEncodeIgnoreSlashes(null)).isEqualTo(null);
    assertThat(urlEncodeIgnoreSlashes("")).isEqualTo("");
    assertThat(urlEncodeIgnoreSlashes(nonEncodedCharacters)).isEqualTo(nonEncodedCharacters);
    assertThat(urlEncodeIgnoreSlashes(encodedCharactersInput)).isEqualTo(encodedCharactersOutput);
  }

}
