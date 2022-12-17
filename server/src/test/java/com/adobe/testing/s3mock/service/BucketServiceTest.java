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

import static com.adobe.testing.s3mock.S3Exception.BUCKET_ALREADY_OWNED_BY_YOU;
import static com.adobe.testing.s3mock.S3Exception.BUCKET_NOT_EMPTY;
import static com.adobe.testing.s3mock.S3Exception.INVALID_BUCKET_NAME;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_ENCODINGTYPE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_MAXKEYS;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_BUCKET;
import static com.adobe.testing.s3mock.service.BucketService.collapseCommonPrefixes;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.MultipartStore;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(classes = {ServiceConfiguration.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockBean({ObjectService.class, MultipartService.class, MultipartStore.class})
class BucketServiceTest extends ServiceTestBase {
  private static final String TEST_BUCKET_NAME = "test-bucket";

  @Autowired
  BucketService iut;

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
    final List<S3Object> result = iut.getS3Objects(TEST_BUCKET_NAME, prefix);
    assertThat(result).hasSize(1);
    assertThat(result.get(0).key()).isEqualTo(key);
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
    List<S3Object> bucketContents = givenBucketContents(prefix);
    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);

    List<S3Object> filteredBucketContents =
        BucketService.filterObjectsBy(bucketContents, commonPrefixes);

    String[] expectedPrefixes = parameters.expectedPrefixes;
    String[] expectedKeys = parameters.expectedKeys;

    assertThat(commonPrefixes).hasSize(expectedPrefixes.length);

    assertThat(commonPrefixes)
        .as("Returned prefixes are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedPrefixes));

    assertThat(filteredBucketContents.stream().map(S3Object::key).collect(toList()))
        .as("Returned keys are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedKeys));
  }

  @Test
  void testCommonPrefixesNoPrefixNoDelimiter() {
    String prefix = "";
    String delimiter = "";
    List<S3Object> bucketContents = givenBucketContents();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesPrefixNoDelimiter() {
    String prefix = "prefix-a";
    String delimiter = "";
    List<S3Object> bucketContents = givenBucketContents();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesNoPrefixDelimiter() {
    String prefix = "";
    String delimiter = "/";
    List<S3Object> bucketContents = givenBucketContents();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/");
  }

  @Test
  void testCommonPrefixesPrefixDelimiter() {
    String prefix = "3330";
    String delimiter = "/";
    List<S3Object> bucketContents = givenBucketContents();

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/");
  }

  @Test
  void testListObjectsV2() {
    String bucketName = "bucket";
    String prefix = null;
    String delimiter = null;
    String encodingType = "url";
    String startAfter = null;
    int maxKeys = 10; //of 14
    String continuationToken = null;
    givenBucketWithContents(bucketName, prefix);
    ListBucketResultV2 listBucketResult =
        iut.listObjectsV2(bucketName, prefix, delimiter, encodingType, startAfter, maxKeys,
            continuationToken);
    assertThat(listBucketResult).isNotNull();
    assertThat(listBucketResult.name()).isEqualTo(bucketName);
    assertThat(listBucketResult.prefix()).isEqualTo(prefix);
    assertThat(listBucketResult.startAfter()).isEqualTo(startAfter);
    assertThat(listBucketResult.encodingType()).isEqualTo(encodingType);
    assertThat(listBucketResult.isTruncated()).isEqualTo(true);
    assertThat(listBucketResult.maxKeys()).isEqualTo(maxKeys);
    assertThat(listBucketResult.nextContinuationToken()).isNotEmpty();
    assertThat(listBucketResult.contents()).hasSize(maxKeys);
  }

  @Test
  void testListObjectsV1() {
    String bucketName = "bucket";
    String prefix = null;
    String delimiter = null;
    String marker = null;
    String encodingType = "url";
    int maxKeys = 10; //of 14
    givenBucketWithContents(bucketName, prefix);
    ListBucketResult listBucketResult =
        iut.listObjectsV1(bucketName, prefix, delimiter, marker, encodingType, maxKeys);
    assertThat(listBucketResult).isNotNull();
    assertThat(listBucketResult.name()).isEqualTo(bucketName);
    assertThat(listBucketResult.prefix()).isEqualTo(prefix);
    assertThat(listBucketResult.marker()).isEqualTo(marker);
    assertThat(listBucketResult.encodingType()).isEqualTo(encodingType);
    assertThat(listBucketResult.isTruncated()).isEqualTo(true);
    assertThat(listBucketResult.maxKeys()).isEqualTo(maxKeys);
    assertThat(listBucketResult.nextMarker()).isEqualTo("c/1/1");
    assertThat(listBucketResult.contents()).hasSize(maxKeys);
  }

  @Test
  void testVerifyBucketExists_success() {
    String bucketName = "bucket";
    when(bucketStore.doesBucketExist(bucketName)).thenReturn(true);
    iut.verifyBucketExists(bucketName);
  }

  @Test
  void testVerifyBucketExists_failure() {
    String bucketName = "bucket";
    givenBucket(bucketName);
    when(bucketStore.doesBucketExist(bucketName)).thenReturn(false);
    assertThatThrownBy(() -> iut.verifyBucketExists(bucketName)).isEqualTo(NO_SUCH_BUCKET);
  }

  @Test
  void testVerifyBucketObjectLockEnabled_success() {
    String bucketName = "bucket";
    when(bucketStore.isObjectLockEnabled(bucketName)).thenReturn(true);
    iut.verifyBucketObjectLockEnabled(bucketName);
  }

  @Test
  void testVerifyBucketObjectLockEnabled_failure() {
    String bucketName = "bucket";
    givenBucket(bucketName);
    when(bucketStore.isObjectLockEnabled(bucketName)).thenReturn(false);
    assertThatThrownBy(() -> iut.verifyBucketObjectLockEnabled(bucketName))
        .isEqualTo(NOT_FOUND_BUCKET_OBJECT_LOCK);
  }

  @Test
  void testVerifyBucketNameIsAllowed_success() {
    String bucketName = "bucket";
    iut.verifyBucketNameIsAllowed(bucketName);
  }

  @Test
  void testVerifyBucketNameIsAllowed_failure() {
    String bucketName = "!!!bucketNameNotAllowed!!!";
    givenBucket(bucketName);
    assertThatThrownBy(() -> iut.verifyBucketNameIsAllowed(bucketName))
        .isEqualTo(INVALID_BUCKET_NAME);
  }

  @Test
  void testVerifyBucketDoesNotExist_success() {
    String bucketName = "bucket";
    iut.verifyBucketDoesNotExist(bucketName);
    verify(bucketStore).doesBucketExist(bucketName);
  }

  @Test
  void testVerifyBucketDoesNotExist_failure() {
    String bucketName = "bucket";
    givenBucket(bucketName);
    assertThatThrownBy(() -> iut.verifyBucketDoesNotExist(bucketName))
        .isEqualTo(BUCKET_ALREADY_OWNED_BY_YOU);
  }

  @Test
  void testVerifyBucketIsEmpty_success() {
    String bucketName = "bucket";
    when(bucketStore.isBucketEmpty(bucketName)).thenReturn(true);
    iut.verifyBucketIsEmpty(bucketName);
  }

  @Test
  void testVerifyBucketIsEmpty_failure() {
    String bucketName = "bucket";
    givenBucket(bucketName);
    when(bucketStore.isBucketEmpty(bucketName)).thenReturn(false);
    assertThatThrownBy(() -> iut.verifyBucketIsEmpty(bucketName)).isEqualTo(BUCKET_NOT_EMPTY);
  }

  @Test
  void testVerifyMaxKeys_success() {
    int keys = 10;
    iut.verifyMaxKeys(keys);
  }

  @Test
  void testVerifyMaxKeys_failure() {
    int keys = -1;
    assertThatThrownBy(() -> {
          iut.verifyMaxKeys(keys);
        }
    ).isEqualTo(INVALID_REQUEST_MAXKEYS);
  }

  @Test
  void testVerifyEncodingType_success() {
    String encodingType = "url";
    iut.verifyEncodingType(encodingType);
  }

  @Test
  void testVerifyEncodingType_failure() {
    String encodingType = "not-url";
    assertThatThrownBy(() -> iut.verifyEncodingType(encodingType))
        .isEqualTo(INVALID_REQUEST_ENCODINGTYPE);
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
