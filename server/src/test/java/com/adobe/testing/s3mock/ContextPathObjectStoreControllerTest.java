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
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.Buckets;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.MultipartStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
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

@MockBean(classes = {KmsKeyStore.class, ObjectService.class,
    MultipartService.class, MultipartStore.class})
@SpringBootTest(properties = {"com.adobe.testing.s3mock.contextPath=s3-mock"},
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ContextPathObjectStoreControllerTest {
  private static final Owner TEST_OWNER = new Owner("123", "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();

  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private BucketService bucketService;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testListBuckets_Ok() throws Exception {
    var bucketList = List.of(TEST_BUCKET,
      new Bucket(Paths.get("/tmp/foo/2"), "testBucket1", Instant.now().toString())
    );
    var expected =
        new ListAllMyBucketsResult(TEST_OWNER, new Buckets(bucketList));
    when(bucketService.listBuckets()).thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/s3-mock/",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }
}


