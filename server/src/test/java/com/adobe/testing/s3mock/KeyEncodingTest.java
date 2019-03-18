/*
 *  Copyright 2017-2019 Adobe.
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

import static com.adobe.testing.s3mock.FileStoreController.fileNameToObjectName;
import static com.adobe.testing.s3mock.FileStoreController.objectNameToFileName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;


public class KeyEncodingTest {
  @Test
  public void testKeyEncoding() {
    assertThat("single char (0x00) encoding", 
        objectNameToFileName("" + (char) 0x00), equalTo("%0000"));
    assertThat("single char (':') encoding", 
        objectNameToFileName(":"), equalTo("%003A"));
    assertThat("single char (\\u12AB) encoding", 
        objectNameToFileName("" + (char) 0x12ab), equalTo("%12AB"));
    assertThat("multiple chars encoding", 
        objectNameToFileName((char) 0x00 + ":<>%" + (char) 0x7f), 
        equalTo("%0000%003A%003C%003E%0025%007F"));
    assertThat("mixed encoding", 
        objectNameToFileName("foo" + (char) 0x00 + "bar:%baz"), 
        equalTo("foo%0000bar%003A%0025baz"));
  }
  
  @Test
  public void testKeyDecoding() {
    assertThat("single char (0x00) decoding", 
        fileNameToObjectName("%0000"), equalTo("" + (char) 0x00));
    assertThat("single char (':') decoding", 
        fileNameToObjectName("%003A"), equalTo(":"));
    assertThat("single char (\\u12AB) decoding", 
        fileNameToObjectName("%12AB"), equalTo("" + (char) 0x12ab));
    assertThat("multiple chars encoding", 
        fileNameToObjectName("%0000%003A%003C%003E%0025%007F"), 
        equalTo((char) 0x00 + ":<>%" + (char) 0x7f));
    assertThat("mixed encoding", 
        fileNameToObjectName("foo%0000bar%003A%0025baz"), 
        equalTo("foo" + (char) 0x00 + "bar:%baz"));
  }
}
