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
import java.util.List;
import org.junit.jupiter.api.Test;

class TaggingHeaderConverterTest {

  @Test
  void testSingleTagConversion() {
    TaggingHeaderConverter iut = new TaggingHeaderConverter();
    String singleTag = "tag1=value1";
    List<Tag> actual = iut.convert(singleTag);
    assertThat(actual).isNotEmpty().hasSize(1);
    assertThat(actual.get(0)).isEqualTo(new Tag(singleTag));
  }

  @Test
  void testMultipleTagConversion() {
    TaggingHeaderConverter iut = new TaggingHeaderConverter();
    String tag1 = "tag1=value1";
    String tag2 = "tag2=value2";
    List<Tag> actual = iut.convert(tag1 + "&" + tag2);
    assertThat(actual).isNotEmpty().hasSize(2);
    assertThat(actual.get(0)).isEqualTo(new Tag(tag1));
    assertThat(actual.get(1)).isEqualTo(new Tag(tag2));
  }

}
