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

import static com.adobe.testing.s3mock.S3Exception.BUCKET_NOT_EMPTY;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_ENCODINGTYPE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_MAXKEYS;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_BUCKET;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = {KmsKeyStore.class, BucketStore.class, ObjectStore.class, ObjectService.class,
    MultipartService.class})
@SpringBootTest(classes = {S3MockConfiguration.class})
class BucketControllerTest {

  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private BucketService bucketService;

  @Autowired
  private MockMvc mockMvc;

  @Test
  void testListBuckets_Ok() throws Exception {
    List<Bucket> bucketList = new ArrayList<>();
    bucketList.add(TEST_BUCKET);
    bucketList.add(new Bucket(Paths.get("/tmp/foo/2"), "test-bucket1", Instant.now().toString()));
    ListAllMyBucketsResult expected = new ListAllMyBucketsResult(TEST_OWNER, bucketList);
    when(bucketService.listBuckets()).thenReturn(expected);

    mockMvc.perform(
            get("/")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testListBuckets_Empty() throws Exception {
    ListAllMyBucketsResult expected =
        new ListAllMyBucketsResult(TEST_OWNER, Collections.emptyList());
    when(bucketService.listBuckets()).thenReturn(expected);

    mockMvc.perform(
            get("/")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testHeadBucket_Ok() throws Exception {
    when(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        head("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testHeadBucket_NotFound() throws Exception {
    doThrow(NO_SUCH_BUCKET)
        .when(bucketService).verifyBucketExists(anyString());

    mockMvc.perform(
        head("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testCreateBucket_Ok() throws Exception {
    mockMvc.perform(
        put("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testCreateBucket_InternalServerError() throws Exception {
    when(bucketService.createBucket(TEST_BUCKET_NAME))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    mockMvc.perform(
        put("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testDeleteBucket_NoContent() throws Exception {
    givenBucket();
    when(bucketService.isBucketEmpty(TEST_BUCKET_NAME)).thenReturn(true);
    when(bucketService.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNoContent());
  }

  @Test
  void testDeleteBucket_NotFound() throws Exception {
    doThrow(NO_SUCH_BUCKET)
        .when(bucketService).verifyBucketIsEmpty(anyString());

    ErrorResponse errorResponse = from(NO_SUCH_BUCKET);

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testDeleteBucket_Conflict() throws Exception {
    givenBucket();
    doThrow(BUCKET_NOT_EMPTY)
        .when(bucketService).verifyBucketIsEmpty(anyString());
    ErrorResponse errorResponse = from(BUCKET_NOT_EMPTY);

    when(bucketService.getS3Objects(TEST_BUCKET_NAME, null))
        .thenReturn(Collections.singletonList(new S3Object()));

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isConflict())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testDeleteBucket_InternalServerError() throws Exception {
    givenBucket();

    doThrow(new IllegalStateException("THIS IS EXPECTED"))
        .when(bucketService).verifyBucketIsEmpty(anyString());

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }


  @Test
  void testListObjectsV1_BadRequest() throws Exception {
    givenBucket();

    int maxKeys = -1;
    doThrow(INVALID_REQUEST_MAXKEYS).when(bucketService).verifyMaxKeys(maxKeys);
    ErrorResponse maxKeysError = from(INVALID_REQUEST_MAXKEYS);
    String encodingtype = "not_valid";
    doThrow(INVALID_REQUEST_ENCODINGTYPE).when(bucketService).verifyEncodingType(encodingtype);
    ErrorResponse encodingTypeError = from(INVALID_REQUEST_ENCODINGTYPE);

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .queryParam(MAX_KEYS, String.valueOf(maxKeys))
    ).andExpect(MockMvcResultMatchers.status().isBadRequest())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(maxKeysError)));

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .queryParam(ENCODING_TYPE, encodingtype)
    ).andExpect(MockMvcResultMatchers.status().isBadRequest())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(encodingTypeError)));
  }

  @Test
  void testListObjectsV2_BadRequest() throws Exception {
    givenBucket();

    int maxKeys = -1;
    doThrow(INVALID_REQUEST_MAXKEYS).when(bucketService).verifyMaxKeys(maxKeys);
    ErrorResponse maxKeysError = from(INVALID_REQUEST_MAXKEYS);
    String encodingtype = "not_valid";
    doThrow(INVALID_REQUEST_ENCODINGTYPE).when(bucketService).verifyEncodingType(encodingtype);
    ErrorResponse encodingTypeError = from(INVALID_REQUEST_ENCODINGTYPE);

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .param("list-type", "2")
            .queryParam(MAX_KEYS, String.valueOf(maxKeys))
    ).andExpect(MockMvcResultMatchers.status().isBadRequest())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(maxKeysError)));

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .param("list-type", "2")
            .queryParam(ENCODING_TYPE, encodingtype)
    ).andExpect(MockMvcResultMatchers.status().isBadRequest())
    .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(encodingTypeError)));
  }

  @Test
  void testListObjectsV1_InternalServerError() throws Exception {
    givenBucket();
    when(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testListObjectsV2_InternalServerError() throws Exception {
    givenBucket();
    when(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    mockMvc.perform(
        get("/test-bucket")
            .param("list-type", "2")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testListObjectsV1_Ok() throws Exception {
    givenBucket();
    String key = "key";
    S3Object s3Object = bucketContents(key);
    ListBucketResult expected =
        new ListBucketResult(TEST_BUCKET_NAME, null, null, 1000, false, null, null,
            Collections.singletonList(s3Object), Collections.emptyList());

    when(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
        .thenReturn(expected);

    mockMvc.perform(
            get("/test-bucket")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testListObjectsV2_Ok() throws Exception {
    givenBucket();
    String key = "key";
    S3Object s3Object = bucketContents(key);
    ListBucketResultV2 expected =
        new ListBucketResultV2(TEST_BUCKET_NAME, null, 1000, false,
            Collections.singletonList(s3Object), Collections.emptyList(),
            null, null, null, null, null);

    when(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
        .thenReturn(expected);

    mockMvc.perform(
            get("/test-bucket")
                .param("list-type", "2")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  private S3Object bucketContents(String id) {
    return new S3Object(id, "1234", "etag", "size", StorageClass.STANDARD, TEST_OWNER);
  }

  private void givenBucket() {
    when(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
  }

  private ErrorResponse from(S3Exception e) {
    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode(e.getCode());
    errorResponse.setMessage(e.getMessage());
    return errorResponse;
  }
}
