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
import static org.springframework.http.MediaType.APPLICATION_JSON;

import com.adobe.testing.s3mock.BucketController;
import com.adobe.testing.s3mock.MultipartController;
import com.adobe.testing.s3mock.ObjectController;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@MockBean(classes = {KmsKeyStore.class,
    ObjectController.class,
    BucketController.class,
    MultipartController.class
})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FaviconControllerTest {
  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testFavicon() {
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_JSON));
    ResponseEntity<String> response = restTemplate.exchange(
        "/favicon.ico",
        HttpMethod.POST,
        new HttpEntity<>(headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }
}
