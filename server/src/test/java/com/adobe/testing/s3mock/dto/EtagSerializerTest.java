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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EtagSerializerTest {

  Writer jsonWriter;
  JsonGenerator jsonGenerator;
  SerializerProvider serializerProvider;

  @BeforeEach
  void before() throws IOException {
    jsonWriter = new StringWriter();
    jsonGenerator = new JsonFactory().createGenerator(jsonWriter);
    serializerProvider = new ObjectMapper().getSerializerProvider();
  }

  @Test
  void testSerializeEtag() throws IOException {
    EtagSerializer iut = new EtagSerializer();
    iut.serialize("some-etag", jsonGenerator, serializerProvider);
    jsonGenerator.flush();
    assertThat(jsonWriter.toString()).isEqualTo("\"\\\"some-etag\\\"\"");
  }

  @Test
  void testSerializeQuotedEtag() throws IOException {
    EtagSerializer iut = new EtagSerializer();
    iut.serialize("\"some-etag\"", jsonGenerator, serializerProvider);
    jsonGenerator.flush();
    assertThat(jsonWriter.toString()).isEqualTo("\"\\\"some-etag\\\"\"");
  }

}
