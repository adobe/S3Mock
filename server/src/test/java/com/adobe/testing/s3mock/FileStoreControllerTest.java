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

import static com.adobe.testing.s3mock.FileStoreController.collapseCommonPrefixes;
import static com.adobe.testing.s3mock.FileStoreController.filterBucketContentsBy;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.BucketContents;
import com.adobe.testing.s3mock.dto.Buckets;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
@SpringBootTest(classes = {S3MockConfiguration.class})
class FileStoreControllerTest {
  //verbatim copy from FileStoreController / FileStore
  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String[] ALL_OBJECTS =
      new String[] {"3330/0", "33309/0", "a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};

  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private KmsKeyStore kmsKeyStore; //Dependency of S3MockConfiguration.

  @MockBean
  private FileStore fileStore;

  @Autowired
  private MockMvc mockMvc;

  @Test
  void testListBuckets_Ok() throws Exception {
    List<Bucket> bucketList = new ArrayList<>();
    bucketList.add(TEST_BUCKET);
    bucketList.add(new Bucket(Paths.get("/tmp/foo/2"), "testBucket1", Instant.now().toString()));
    when(fileStore.listBuckets()).thenReturn(bucketList);

    ListAllMyBucketsResult expected = new ListAllMyBucketsResult();
    Buckets buckets = new Buckets();
    buckets.setBuckets(bucketList);
    expected.setBuckets(buckets);
    expected.setOwner(TEST_OWNER);

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
    when(fileStore.listBuckets()).thenReturn(Collections.emptyList());

    ListAllMyBucketsResult expected = new ListAllMyBucketsResult();
    expected.setOwner(TEST_OWNER);

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
    when(fileStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        head("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testHeadBucket_NotFound() throws Exception {
    when(fileStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(false);

    mockMvc.perform(
        head("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testCreateBucket_Ok() throws Exception {
    mockMvc.perform(
        put("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testCreateBucket_InternalServerError() throws Exception {
    when(fileStore.createBucket(TEST_BUCKET_NAME))
        .thenThrow(new RuntimeException("THIS IS EXPECTED"));

    mockMvc.perform(
        put("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testDeleteBucket_NoContent() throws Exception {
    when(fileStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null)).thenReturn(Collections.emptyList());

    when(fileStore.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        delete("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNoContent());
  }

  @Test
  void testDeleteBucket_NotFound() throws Exception {
    when(fileStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null)).thenReturn(Collections.emptyList());

    when(fileStore.deleteBucket(TEST_BUCKET_NAME)).thenReturn(false);

    mockMvc.perform(
        delete("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testDeleteBucket_Conflict() throws Exception {
    when(fileStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null))
        .thenReturn(Collections.singletonList(new S3Object()));

    mockMvc.perform(
        delete("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isConflict());
  }

  @Test
  void testDeleteBucket_InternalServerError() throws Exception {
    when(fileStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null))
        .thenThrow(new IOException("THIS IS EXPECTED"));

    mockMvc.perform(
        delete("/testBucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  /**
   * Parameter factory.
   * Taken from ListObjectIT to make sure we unit test against the same data.
   */
  public static Iterable<Param> data() {
    return Arrays.asList(
        param(null, null).keys(ALL_OBJECTS),
        param("", null).keys(ALL_OBJECTS),
        param(null, "").keys(ALL_OBJECTS),
        param(null, "/").keys("a", "b", "d:1", "d:1:1", "eor.txt")
            .prefixes("3330/", "foo/", "c/", "b/", "33309/"),
        param("", "").keys(ALL_OBJECTS),
        param("/", null),
        param("b", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b/", null).keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "").keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "/").keys("b").prefixes("b/"),
        param("b/", "/").keys("b/1", "b/2").prefixes("b/1/"),
        param("b/1", "/").keys("b/1").prefixes("b/1/"),
        param("b/1/", "/").keys("b/1/1", "b/1/2"),
        param("c", "/").prefixes("c/"),
        param("c/", "/").keys("c/1").prefixes("c/1/"),
        param("eor", "/").keys("eor.txt")
    );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCommonPrefixesAndBucketContentFilter(final Param parameters) {
    String prefix = parameters.prefix;
    String delimiter = parameters.delimiter;
    List<BucketContents> bucketContents = createBucketContentsList(prefix);
    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);

    List<BucketContents> filteredBucketContents =
        filterBucketContentsBy(bucketContents, commonPrefixes);

    String[] expectedPrefixes = parameters.expectedPrefixes;
    String[] expectedKeys = parameters.expectedKeys;

    assertThat(commonPrefixes).hasSize(expectedPrefixes.length);

    assertThat(commonPrefixes)
        .as("Returned prefixes are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedPrefixes));

    assertThat(filteredBucketContents.stream().map(BucketContents::getKey).collect(toList()))
        .as("Returned keys are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedKeys));
  }

  @Test
  void testCommonPrefixesNoPrefixNoDelimiter() {
    String prefix = "";
    String delimiter = "";
    List<BucketContents> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesPrefixNoDelimiter() {
    String prefix = "prefixa";
    String delimiter = "";
    List<BucketContents> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesNoPrefixDelimiter() {
    String prefix = "";
    String delimiter = "/";
    List<BucketContents> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/");
  }

  @Test
  void testCommonPrefixesPrefixDelimiter() {
    String prefix = "3330";
    String delimiter = "/";
    List<BucketContents> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/");
  }

  List<BucketContents> createBucketContentsList() {
    return createBucketContentsList(null);

  }

  List<BucketContents> createBucketContentsList(String prefix) {
    List<BucketContents> list = new ArrayList<>();
    for (String object : ALL_OBJECTS) {
      if (StringUtils.isNotEmpty(prefix)) {
        if (!object.startsWith(prefix)) {
          continue;
        }
      }
      list.add(createBucketContents(object));
    }
    return list;
  }

  BucketContents createBucketContents(String key) {
    String lastModified = "lastModified";
    String etag = "etag";
    String size = "size";
    String storageClass = "storageClass";
    Owner owner = new Owner(0L, "name");
    return new BucketContents(key, lastModified, etag, size, storageClass, owner);
  }

  static class Param {
    final String prefix;
    final String delimiter;
    String[] expectedPrefixes = new String[0];
    String[] expectedKeys = new String[0];

    private Param(final String prefix, final String delimiter) {
      this.prefix = prefix;
      this.delimiter = delimiter;
    }

    Param prefixes(final String... expectedPrefixes) {
      this.expectedPrefixes = expectedPrefixes;
      return this;
    }

    Param keys(final String... expectedKeys) {
      this.expectedKeys = expectedKeys;
      return this;
    }

    @Override
    public String toString() {
      return String.format("prefix=%s, delimiter=%s", prefix, delimiter);
    }
  }

  static Param param(final String prefix, final String delimiter) {
    return new Param(prefix, delimiter);
  }
}


