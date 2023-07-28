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

package com.adobe.testing.s3mock;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.Tag;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class TaggingHeaderConverterTest {

  @Test
  void testSingleTagConversion() {
    var iut = new TaggingHeaderConverter();
    var singleTag = tag(1);
    var actual = iut.convert(singleTag);
    assertThat(actual).isNotEmpty().hasSize(1);
    assertThat(actual.get(0)).isEqualTo(new Tag(singleTag));
  }

  @Test
  void testMultipleTagsConversion() {
    var iut = new TaggingHeaderConverter();
    var tags = new ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      tags.add(tag(i));
    }
    var actual = iut.convert(String.join("&", tags));
    assertThat(actual)
        .isNotEmpty()
        .hasSize(5)
        .containsOnly(
            new Tag(tag(0)),
            new Tag(tag(1)),
            new Tag(tag(2)),
            new Tag(tag(3)),
            new Tag(tag(4))
        );
  }

  String tag(int i) {
    return String.format("tag%d=value%d", i, i);
  }
}
