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

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_MD5;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_RETAINDATE;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NOT_MODIFIED;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY;
import static com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED;
import static com.adobe.testing.s3mock.service.ObjectService.WILDCARD_ETAG;
import static com.adobe.testing.s3mock.util.DigestUtil.base64Digest;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.MultipartStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(classes = {ServiceConfiguration.class})
@MockBean({BucketService.class, MultipartService.class, MultipartStore.class})
class ObjectServiceTest extends ServiceTestBase {
  private static final String TEST_FILE_PATH = "src/test/resources/sampleFile.txt";
  @Autowired
  ObjectService iut;

  @Test
  void testDeleteObjects() {
    String bucketName = "bucket";
    String key = "key";
    String key2 = "key2";
    givenBucketWithContents(bucketName, "", Arrays.asList(givenS3Object(key),
        givenS3Object(key2)));
    Delete delete = new Delete(false, Arrays.asList(givenS3ObjectIdentifier(key),
        givenS3ObjectIdentifier(key2)));

    when(objectStore.deleteObject(any(BucketMetadata.class), any(UUID.class)))
        .thenReturn(true);
    when(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true);
    when(bucketStore.removeFromBucket(key2, bucketName)).thenReturn(true);
    DeleteResult deleted = iut.deleteObjects(bucketName, delete);
    assertThat(deleted.deletedObjects()).hasSize(2);
  }

  S3ObjectIdentifier givenS3ObjectIdentifier(String key) {
    return new S3ObjectIdentifier(key, null);
  }

  @Test
  void testDeleteObject() {
    String bucketName = "bucket";
    String key = "key";
    givenBucketWithContents(bucketName, "", singletonList(givenS3Object(key)));
    when(objectStore.deleteObject(any(BucketMetadata.class), any(UUID.class)))
        .thenReturn(true);
    when(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true);
    boolean deleted = iut.deleteObject(bucketName, key);
    assertThat(deleted).isTrue();
  }

  @Test
  void testVerifyRetention_success() {
    Retention retention = new Retention(Mode.COMPLIANCE,
        Instant.now().plus(1, MINUTES));

    iut.verifyRetention(retention);
  }

  @Test
  void testVerifyRetention_failure() {
    Retention retention = new Retention(Mode.COMPLIANCE,
        Instant.now().minus(1, MINUTES));
    assertThatThrownBy(() -> iut.verifyRetention(retention)).isEqualTo(INVALID_REQUEST_RETAINDATE);
  }

  @Test
  void testVerifyMd5_success() throws IOException {
    final File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    final String md5 = base64Digest(Files.newInputStream(path));
    InputStream inputStream = iut.verifyMd5(Files.newInputStream(path), md5, null);
    assertThat(base64Digest(inputStream)).isEqualTo(md5);
  }

  @Test
  void testVerifyMd5_failure() {
    final File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    final String md5 = "wrong-md5";
    assertThatThrownBy(() ->
        iut.verifyMd5(Files.newInputStream(path), md5, null)
    ).isEqualTo(BAD_REQUEST_MD5);
  }

  @Test
  void testVerifyMd5Void_success() throws IOException {
    final File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    final String md5 = base64Digest(Files.newInputStream(path));
    iut.verifyMd5(Files.newInputStream(path), md5);
  }

  @Test
  void testVerifyMd5Void_failure() {
    final File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    final String md5 = "wrong-md5";
    assertThatThrownBy(() ->
        iut.verifyMd5(Files.newInputStream(path), md5)
    ).isEqualTo(BAD_REQUEST_MD5);
  }

  @Test
  void testVerifyObjectMatching_matchSuccess() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"someetag\"";

    iut.verifyObjectMatching(singletonList(etag), null, s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_matchWildcard() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"nonematch\"";

    iut.verifyObjectMatching(Arrays.asList(etag, WILDCARD_ETAG), null, s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_matchFailure() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"nonematch\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(singletonList(etag), null, s3ObjectMetadata)
    ).isEqualTo(PRECONDITION_FAILED);
  }

  @Test
  void testVerifyObjectMatching_noneMatchSuccess() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"nonematch\"";

    iut.verifyObjectMatching(null, singletonList(etag), s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_noneMatchWildcard() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"someetag\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(null, Arrays.asList(etag, WILDCARD_ETAG), s3ObjectMetadata)
    ).isEqualTo(NOT_MODIFIED);
  }

  @Test
  void testVerifyObjectMatching_noneMatchFailure() {
    String key = "key";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    String etag = "\"someetag\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(null, singletonList(etag), s3ObjectMetadata)
    ).isEqualTo(NOT_MODIFIED);
  }

  @Test
  void testVerifyObjectLockConfiguration_failure() {
    String bucketName = "bucket";
    String prefix = "";
    String key = "key";
    givenBucketWithContents(bucketName, prefix, singletonList(givenS3Object(key)));
    assertThatThrownBy(() -> iut.verifyObjectLockConfiguration(bucketName, key))
        .isEqualTo(NOT_FOUND_OBJECT_LOCK);
  }

  @Test
  void testVerifyObjectExists_success() {
    String bucketName = "bucket";
    String prefix = "";
    String key = "key";
    givenBucketWithContents(bucketName, prefix, singletonList(givenS3Object(key)));
    S3ObjectMetadata s3ObjectMetadata = iut.verifyObjectExists(bucketName, key);
    assertThat(s3ObjectMetadata.getKey()).isEqualTo(key);
  }

  @Test
  void testVerifyObjectExists_failure() {
    String bucketName = "bucket";
    String key = "key";
    givenBucket(bucketName);
    assertThatThrownBy(() -> iut.verifyObjectExists(bucketName, key)).isEqualTo(NO_SUCH_KEY);
  }
}
