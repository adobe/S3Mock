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

package com.adobe.testing.s3mock.util;

import static java.util.Objects.requireNonNull;

import com.adobe.testing.s3mock.dto.Range;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.core.convert.converter.Converter;

/**
 * Converts http range header value to range object
 */
public class RangeConverter implements Converter<String, Range> {
  private static final String REQUESTED_RANGE_REGEXP = "^bytes=((\\d*)\\-(\\d*))((,\\d*-\\d*)*)";

  private static final Pattern REQUESTED_RANGE_PATTERN = Pattern.compile(REQUESTED_RANGE_REGEXP);

  @Override
  public Range convert(String rangeString) {
    requireNonNull(rangeString);

    final Range range;

    // parsing a range specification of format: "bytes=start-end" - multiple ranges not supported
    rangeString = rangeString.trim();
    final Matcher matcher = REQUESTED_RANGE_PATTERN.matcher(rangeString);
    if (matcher.matches()) {
      final String rangeStart = matcher.group(2);
      final String rangeEnd = matcher.group(3);

      range =
          new Range(rangeStart == null ? 0L : Long.parseLong(rangeStart),
              (rangeEnd == null ? Long.MAX_VALUE
                  : Long.parseLong(rangeEnd)));

      if (matcher.groupCount() == 5 && !"".equals(matcher.group(4))) {
        throw new IllegalArgumentException(
            "Unsupported range specification. Only single range specifications allowed");
      }
      if (range.getStart() < 0) {
        throw new IllegalArgumentException(
            "Unsupported range specification. A start byte must be supplied");
      }

      if (range.getEnd() != -1 && range.getEnd() < range.getStart()) {
        throw new IllegalArgumentException(
            "Range header is malformed. End byte is smaller than start byte.");
      }
    } else {
      throw new IllegalArgumentException(
          "Range header is malformed. Only bytes supported as range type.");
    }

    return range;
  }
}
