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

package com.adobe.testing.s3mock.dto;

/**
 * Range request value object
 */
public class Range {
  private final long start;

  private final long end;

  /**
   * Constructs a new {@link Range}.
   *
   * @param start of range
   * @param end of range
   */
  public Range(final long start, final long end) {
    this.start = start;
    this.end = end;
  }

  /**
   * @return start index of range request
   */
  public long getStart() {
    return start;
  }

  /**
   * @return end index of range request
   */
  public long getEnd() {
    return end;
  }
}
