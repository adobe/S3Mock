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

package com.adobe.testing.s3mock.dto;

import static com.adobe.testing.s3mock.dto.DtoTestUtil.deserialize;
import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class TaggingTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    var iut = new Tagging(new TagSet(List.of(createTag(0), createTag(1))));
    assertThat(iut).isNotNull();
    serializeAndAssert(iut, testInfo);
  }

  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    var iut = deserialize(Tagging.class, testInfo);
    assertThat(iut.tagSet().tags()).hasSize(2);

    var tag0 = iut.tagSet().tags().get(0);
    assertThat(tag0.key()).isEqualTo("key0");
    assertThat(tag0.value()).isEqualTo("val0");

    var tag1 = iut.tagSet().tags().get(1);
    assertThat(tag1.key()).isEqualTo("key1");
    assertThat(tag1.value()).isEqualTo("val1");
  }

  private static Tag createTag(int counter) {
    return new Tag("key" + counter, "val" + counter);
  }
}
