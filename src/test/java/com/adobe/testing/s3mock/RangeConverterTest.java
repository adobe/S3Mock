/*
 *  Copyright 2017 Adobe.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.util.RangeConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 *
 */
@RunWith(JUnit4.class)
@SuppressWarnings("javadoc")
public class RangeConverterTest {
  @Test
  public void convertsValidRange() {
    final String rangeRequest = "bytes=10-35";
    final Range range = new RangeConverter().convert(rangeRequest);

    assertThat("bad range start", range.getStart(), equalTo(10L));
    assertThat("bad range end", range.getEnd(), equalTo(35L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsExceptionOnNegative() {
    final String rangeRequest = "bytes=2-1";
    new RangeConverter().convert(rangeRequest);

  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidRangeString() {
    final String rangeRequest = "bytes=a-b";
    new RangeConverter().convert(rangeRequest);

  }
}
