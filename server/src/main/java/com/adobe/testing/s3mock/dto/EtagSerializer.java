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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

/**
 * ETag values must be wrapped in quotation marks.
 * Example: {@code <ETag>"etag-value"</ETag>}
 */
public class EtagSerializer extends JsonSerializer<String> {

  @Override
  public void serialize(String s, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    String toSerialize = s;
    if (!StringUtils.startsWith(s, "\"")) {
      toSerialize = String.format("\"%s\"", s);
    }
    jsonGenerator.writeString(toSerialize);
  }
}
