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

import java.util.List;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpRange;

public class HttpRangeHeaderConverter implements Converter<String, HttpRange> {

  @Override
  public HttpRange convert(String source) {
    HttpRange range = null;
    List<HttpRange> httpRanges = HttpRange.parseRanges(source);
    if (!httpRanges.isEmpty()) {
      range = httpRanges.get(0);
    }
    return range;
  }
}
