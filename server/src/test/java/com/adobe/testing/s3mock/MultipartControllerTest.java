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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.util.UriComponentsBuilder;

@MockBeans({@MockBean(classes = {KmsKeyStore.class, ObjectService.class,
  ObjectController.class, BucketController.class})})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MultipartControllerTest {
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private BucketService bucketService;
  @MockBean
  private MultipartService multipartService;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testCompleteMultipart_BadRequest_uploadTooSmall() throws Exception {
    givenBucket();
    var parts = List.of(
        createPart(0, 5L),
        createPart(1, 5L)
    );

    var uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (var part : parts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag(),
          null, null, null, null));
    }

    var key = "sampleFile.txt";
    var uploadId = "testUploadId";
    doThrow(ENTITY_TOO_SMALL)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam("uploadId", uploadId).build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.POST,
        new HttpEntity<>(MAPPER.writeValueAsString(uploadRequest), headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(from(ENTITY_TOO_SMALL)));
  }

  @Test
  void testCompleteMultipart_BadRequest_uploadIdNotFound() throws Exception {
    givenBucket();
    var uploadId = "testUploadId";

    var parts = List.of(
        createPart(0, 5L),
        createPart(1, 5L)
    );

    doThrow(NO_SUCH_UPLOAD_MULTIPART)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), anyString(), eq(uploadId), anyList());

    var uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (var part : parts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag(),
          null, null, null, null));
    }

    var key = "sampleFile.txt";

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam("uploadId", uploadId).build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.POST,
        new HttpEntity<>(MAPPER.writeValueAsString(uploadRequest), headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(response.getBody())
        .isEqualTo(MAPPER.writeValueAsString(from(NO_SUCH_UPLOAD_MULTIPART)));
  }

  @Test
  void testCompleteMultipart_BadRequest_partNotFound() throws Exception {
    givenBucket();
    var key = "sampleFile.txt";
    var uploadId = "testUploadId";

    var requestParts = List.of(createPart(1, 5L));

    doThrow(INVALID_PART)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());

    var uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (var part : requestParts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag(),
          null, null, null, null));
    }

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam("uploadId", uploadId).build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.POST,
        new HttpEntity<>(MAPPER.writeValueAsString(uploadRequest), headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(from(INVALID_PART)));
  }

  @Test
  void testCompleteMultipart_BadRequest_invalidPartOrder() throws Exception {
    givenBucket();

    var key = "sampleFile.txt";
    var uploadId = "testUploadId";

    doThrow(INVALID_PART_ORDER)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());

    var requestParts = List.of(
        createPart(1, 5L),
        createPart(0, 5L)
    );

    var uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (var part : requestParts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag(),
          null, null, null, null));
    }

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam("uploadId", uploadId).build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.POST,
        new HttpEntity<>(MAPPER.writeValueAsString(uploadRequest), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(from(INVALID_PART_ORDER)));
  }

  private Part createPart(int partNumber, long size) {
    return new Part(partNumber, "someEtag" + partNumber, new Date(), size);
  }

  private void givenBucket() {
    when(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
    when(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);
  }

  private ErrorResponse from(S3Exception e) {
    return new ErrorResponse(
        e.getCode(),
        e.getMessage(),
        null,
        null
    );
  }
}
