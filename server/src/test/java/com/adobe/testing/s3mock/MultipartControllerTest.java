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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.BucketStore;
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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBeans({@MockBean(classes = {KmsKeyStore.class, BucketStore.class, ObjectService.class,
  ObjectController.class, BucketController.class})})
@SpringBootTest(classes = {S3MockConfiguration.class})
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
  private MockMvc mockMvc;

  @Test
  void testCompleteMultipart_BadRequest_uploadTooSmall() throws Exception {
    givenBucket();
    List<Part> parts = new ArrayList<>();
    parts.add(createPart(0, 5L));
    parts.add(createPart(1, 5L));

    CompleteMultipartUpload uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (Part part : parts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag()));
    }

    String key = "sampleFile.txt";
    String uploadId = "testUploadId";
    doThrow(ENTITY_TOO_SMALL)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());
    ErrorResponse errorResponse = from(ENTITY_TOO_SMALL);

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_uploadIdNotFound() throws Exception {
    givenBucket();
    String uploadId = "testUploadId";

    List<Part> parts = new ArrayList<>();
    parts.add(createPart(0, 5L));
    parts.add(createPart(1, 5L));

    doThrow(NO_SUCH_UPLOAD_MULTIPART)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), anyString(), eq(uploadId), anyList());

    CompleteMultipartUpload uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (Part part : parts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag()));
    }
    ErrorResponse errorResponse = from(NO_SUCH_UPLOAD_MULTIPART);

    String key = "sampleFile.txt";
    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isNotFound())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_partNotFound() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";
    String uploadId = "testUploadId";

    List<Part> requestParts = new ArrayList<>();
    requestParts.add(createPart(1, 5L));

    doThrow(INVALID_PART)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());

    CompleteMultipartUpload uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (Part part : requestParts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag()));
    }
    ErrorResponse errorResponse = from(INVALID_PART);

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_invalidPartOrder() throws Exception {
    givenBucket();

    String key = "sampleFile.txt";
    String uploadId = "testUploadId";

    doThrow(INVALID_PART_ORDER)
        .when(multipartService)
        .verifyMultipartParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), anyList());

    List<Part> requestParts = new ArrayList<>();
    requestParts.add(createPart(1, 5L));
    requestParts.add(createPart(0, 5L));

    CompleteMultipartUpload uploadRequest = new CompleteMultipartUpload(new ArrayList<>());
    for (Part part : requestParts) {
      uploadRequest.addPart(new CompletedPart(part.partNumber(), part.etag()));
    }
    ErrorResponse errorResponse = from(INVALID_PART_ORDER);

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
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
