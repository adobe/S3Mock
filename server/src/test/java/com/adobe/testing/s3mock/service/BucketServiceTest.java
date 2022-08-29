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

package com.adobe.testing.s3mock.service;

import static com.adobe.testing.s3mock.service.BucketService.collapseCommonPrefixes;
import static com.adobe.testing.s3mock.service.BucketService.filterBucketContentsBy;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@SpringBootTest(classes = {ServiceConfiguration.class})
@MockBean({MultipartService.class})
class BucketServiceTest {
  private static final String TEST_BUCKET_NAME = "test-bucket";

  @Autowired
  BucketService bucketService;
  @MockBean
  BucketStore bucketStore;
  @MockBean
  ObjectStore objectStore;

  @Test
  void getObject() {
    assertPrefix("a/b/c", "a/b/c");
  }

  @Test
  void getObjectsForParentDirectory() {
    assertPrefix("a/b/c", "a/b");
  }

  @Test
  void getObjectsForPartialPrefix() {
    assertPrefix("foo_bar_baz", "foo");
  }

  @Test
  void getObjectsForEmptyPrefix() {
    assertPrefix("a", "");
  }

  @Test
  void getObjectsForNullPrefix() {
    assertPrefix("a", null);
  }

  @Test
  void getObjectsForPartialParentDirectory() {
    assertPrefix("a/bee/c", "a/b");
  }

  void assertPrefix(String key, String prefix) {
    UUID id = UUID.randomUUID();
    when(bucketStore.lookupKeysInBucket(prefix, TEST_BUCKET_NAME)).thenReturn(singletonList(id));
    BucketMetadata bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    when(bucketStore.getBucketMetadata(TEST_BUCKET_NAME)).thenReturn(bucketMetadata);
    when(objectStore.getS3ObjectMetadata(bucketMetadata, id)).thenReturn(s3ObjectMetadata(id, key));
    final List<S3Object> result = bucketService.getS3Objects(TEST_BUCKET_NAME, prefix);
    assertThat(result).hasSize(1);
    assertThat(result.get(0).getKey()).isEqualTo(key);
  }

  private static final String[] ALL_OBJECTS =
      new String[] {"3330/0", "33309/0", "a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};


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
    List<S3Object> bucketContents = createBucketContentsList(prefix);
    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);

    List<S3Object> filteredBucketContents = filterBucketContentsBy(bucketContents, commonPrefixes);

    String[] expectedPrefixes = parameters.expectedPrefixes;
    String[] expectedKeys = parameters.expectedKeys;

    assertThat(commonPrefixes).hasSize(expectedPrefixes.length);

    assertThat(commonPrefixes)
        .as("Returned prefixes are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedPrefixes));

    assertThat(filteredBucketContents.stream().map(S3Object::getKey).collect(toList()))
        .as("Returned keys are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedKeys));
  }

  @Test
  void testCommonPrefixesNoPrefixNoDelimiter() {
    String prefix = "";
    String delimiter = "";
    List<S3Object> bucketContents = createBucketContentsList();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesPrefixNoDelimiter() {
    String prefix = "prefix-a";
    String delimiter = "";
    List<S3Object> bucketContents = createBucketContentsList();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesNoPrefixDelimiter() {
    String prefix = "";
    String delimiter = "/";
    List<S3Object> bucketContents = createBucketContentsList();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/");
  }

  @Test
  void testCommonPrefixesPrefixDelimiter() {
    String prefix = "3330";
    String delimiter = "/";
    List<S3Object> bucketContents = createBucketContentsList();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/");
  }

  List<S3Object> createBucketContentsList() {
    return createBucketContentsList(null);

  }

  List<S3Object> createBucketContentsList(String prefix) {
    List<S3Object> list = new ArrayList<>();
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

  S3Object createBucketContents(String key) {
    String lastModified = "lastModified";
    String etag = "etag";
    String size = "size";
    Owner owner = new Owner(0L, "name");
    return new S3Object(key, lastModified, etag, size, StorageClass.STANDARD, owner);
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

  private S3ObjectMetadata s3ObjectMetadata(UUID id, String key) {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setId(id);
    s3ObjectMetadata.setKey(key);
    s3ObjectMetadata.setModificationDate("1234");
    s3ObjectMetadata.setEtag("1234");
    s3ObjectMetadata.setSize("size");
    return s3ObjectMetadata;
  }

  private BucketMetadata metadataFrom(String bucketName) {
    BucketMetadata metadata = new BucketMetadata();
    metadata.setName(bucketName);
    metadata.setPath(Paths.get(FileUtils.getTempDirectoryPath(), bucketName));
    return metadata;
  }
}
