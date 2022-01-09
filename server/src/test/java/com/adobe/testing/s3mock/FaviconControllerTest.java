/*
 *  Copyright 2017-2022 Adobe.
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.KmsKeyStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(classes = {S3MockConfiguration.class})
@AutoConfigureMockMvc
class FaviconControllerTest {

  @MockBean
  private KmsKeyStore kmsKeyStore; //Dependency of S3MockConfiguration.

  @MockBean
  private FileStore fileStore; //Dependency of S3MockConfiguration.

  @Autowired
  private MockMvc mvc;

  @Test
  void testFavicon() throws Exception {
    MockHttpServletResponse response =
        mvc.perform(get("/favicon.ico").accept(MediaType.APPLICATION_JSON))
            .andReturn()
            .getResponse();

    assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
  }
}
