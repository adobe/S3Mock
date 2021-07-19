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

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.dto.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RangeTest {

  @Test
  public void convertsValidRange() {
    final String rangeRequest = "bytes=10-35";
    final Range range = new Range(rangeRequest);

    assertThat(range.getStart()).as("bad range start").isEqualTo(10L);
    assertThat(range.getEnd()).as("bad range end").isEqualTo(35L);
  }

  @Test
  public void convertRangeWithRangeEndUndefined() {
    final String rangeRequest = "bytes=10-";
    final Range range = new Range(rangeRequest);

    assertThat(range.getStart()).as("bad range start").isEqualTo(10L);
    assertThat(range.getEnd()).as("bad range end").isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void throwsExceptionOnNegative() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      final String rangeRequest = "bytes=2-1";
      new Range(rangeRequest);
    });

  }

  @Test
  public void invalidRangeString() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      final String rangeRequest = "bytes=a-b";
      new Range(rangeRequest);
    });
  }
}
