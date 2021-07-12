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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class StringEncodingTest {
  @Test
  public void testKeyEncoding() {
    assertThat("single char (0x00) encoding",
        encode("" + (char) 0x00), equalTo("%00"));
    assertThat("single char (':') encoding",
        encode(":"), equalTo("%3A"));
    assertThat("single char (\\u12AB) encoding",
        encode("" + (char) 0x12ab), equalTo("%E1%8A%AB"));
    assertThat("multiple chars encoding",
        encode((char) 0x00 + ":<>%" + (char) 0x7f),
        equalTo("%00%3A%3C%3E%25%7F"));
    assertThat("mixed encoding",
        encode("foo" + (char) 0x00 + "bar:%baz"),
        equalTo("foo%00bar%3A%25baz"));
  }

  @Test
  public void testKeyDecoding() {
    assertThat("single char (0x00) decoding",
        decode("%00"), equalTo("" + (char) 0x00));
    assertThat("single char (':') decoding",
        decode("%3A"), equalTo(":"));
    assertThat("single char (\\u12AB) decoding",
        decode("%E1%8A%AB"), equalTo("" + (char) 0x12ab));
    assertThat("multiple chars encoding",
        decode("%00%3A%3C%3E%25%7F"),
        equalTo((char) 0x00 + ":<>%" + (char) 0x7f));
    assertThat("mixed encoding",
        decode("foo%00bar%3A%25baz"),
        equalTo("foo" + (char) 0x00 + "bar:%baz"));
  }
}
