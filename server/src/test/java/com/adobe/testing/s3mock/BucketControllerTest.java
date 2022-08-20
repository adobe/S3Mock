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

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
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
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBeans({@MockBean(classes = KmsKeyStore.class)})
@SpringBootTest(classes = {S3MockConfiguration.class})
class BucketControllerTest {

  //verbatim copy from FileStoreController / FileStore
  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private FileStore fileStore;
  @MockBean
  private BucketStore bucketStore;

  @Autowired
  private MockMvc mockMvc;

  @Test
  void testListBuckets_Ok() throws Exception {
    List<Bucket> bucketList = new ArrayList<>();
    bucketList.add(TEST_BUCKET);
    bucketList.add(new Bucket(Paths.get("/tmp/foo/2"), "test-bucket1", Instant.now().toString()));
    when(bucketStore.listBuckets()).thenReturn(bucketList);
    ListAllMyBucketsResult expected = new ListAllMyBucketsResult(TEST_OWNER, bucketList);

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
    when(bucketStore.listBuckets()).thenReturn(Collections.emptyList());

    ListAllMyBucketsResult expected =
        new ListAllMyBucketsResult(TEST_OWNER, Collections.emptyList());

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
    when(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        head("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testHeadBucket_NotFound() throws Exception {
    when(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(false);

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
    when(bucketStore.createBucket(TEST_BUCKET_NAME))
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
    when(bucketStore.isBucketEmpty(TEST_BUCKET_NAME)).thenReturn(true);
    when(bucketStore.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNoContent());
  }

  @Test
  void testDeleteBucket_NotFound() throws Exception {
    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testDeleteBucket_Conflict() throws Exception {
    givenBucket();

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null))
        .thenReturn(Collections.singletonList(new S3ObjectMetadata()));

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isConflict());
  }

  @Test
  void testDeleteBucket_InternalServerError() throws Exception {
    givenBucket();

    when(bucketStore.isBucketEmpty(TEST_BUCKET_NAME))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  private void givenBucket() {
    when(bucketStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
    when(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);
  }
}
