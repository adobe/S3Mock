/*
 *  Copyright 2017-2024 Adobe.
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
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.AWS_CHUNKED;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TRAILER;
import static com.adobe.testing.s3mock.util.DigestUtil.base64Digest;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.MultipartStore;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.util.MultiValueMapAdapter;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.internal.chunked.AwsChunkedEncodingConfig;
import software.amazon.awssdk.core.internal.io.AwsUnsignedChunkedEncodingInputStream;

@SpringBootTest(classes = {ServiceConfiguration.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockBean({BucketService.class, MultipartService.class, MultipartStore.class})
class ObjectServiceTest extends ServiceTestBase {
  private static final String TEST_FILE_PATH = "src/test/resources/sampleFile.txt";
  @Autowired
  ObjectService iut;

  @Test
  void testDeleteObjects() {
    var bucketName = "bucket";
    var key = "key";
    var key2 = "key2";
    givenBucketWithContents(bucketName, "", Arrays.asList(givenS3Object(key),
        givenS3Object(key2)));
    var delete = new Delete(false, Arrays.asList(givenS3ObjectIdentifier(key),
        givenS3ObjectIdentifier(key2)));

    when(objectStore.deleteObject(any(BucketMetadata.class), any(UUID.class)))
        .thenReturn(true);
    when(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true);
    when(bucketStore.removeFromBucket(key2, bucketName)).thenReturn(true);
    var deleted = iut.deleteObjects(bucketName, delete);
    assertThat(deleted.deletedObjects()).hasSize(2);
  }

  S3ObjectIdentifier givenS3ObjectIdentifier(String key) {
    return new S3ObjectIdentifier(key, null);
  }

  @Test
  void testDeleteObject() {
    var bucketName = "bucket";
    var key = "key";
    givenBucketWithContents(bucketName, "", singletonList(givenS3Object(key)));
    when(objectStore.deleteObject(any(BucketMetadata.class), any(UUID.class)))
        .thenReturn(true);
    when(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true);
    var deleted = iut.deleteObject(bucketName, key);
    assertThat(deleted).isTrue();
  }

  @Test
  void testVerifyRetention_success() {
    var retention = new Retention(Mode.COMPLIANCE, now().plus(1, MINUTES));

    iut.verifyRetention(retention);
  }

  @Test
  void testVerifyRetention_failure() {
    var retention = new Retention(Mode.COMPLIANCE, now().minus(1, MINUTES));
    assertThatThrownBy(() -> iut.verifyRetention(retention)).isEqualTo(INVALID_REQUEST_RETAINDATE);
  }

  @Test
  void testVerifyMd5_success() throws IOException {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var md5 = base64Digest(Files.newInputStream(path));
    iut.verifyMd5(path, md5);
  }

  @Test
  void testVerifyMd5_failure() {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var md5 = "wrong-md5";
    assertThatThrownBy(() ->
        iut.verifyMd5(path, md5)
    ).isEqualTo(BAD_REQUEST_MD5);
  }

  @Test
  void testVerifyMd5Void_success() throws IOException {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var md5 = base64Digest(Files.newInputStream(path));
    iut.verifyMd5(Files.newInputStream(path), md5);
  }

  @Test
  void testVerifyMd5Void_failure() {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var md5 = "wrong-md5";
    assertThatThrownBy(() ->
        iut.verifyMd5(Files.newInputStream(path), md5)
    ).isEqualTo(BAD_REQUEST_MD5);
  }

  @Test
  void testVerifyObjectMatching_matchSuccess() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"someetag\"";

    iut.verifyObjectMatching(singletonList(etag), null, s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_matchWildcard() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"nonematch\"";

    iut.verifyObjectMatching(Arrays.asList(etag, WILDCARD_ETAG), null, s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_matchFailure() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"nonematch\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(singletonList(etag), null, s3ObjectMetadata)
    ).isEqualTo(PRECONDITION_FAILED);
  }

  @Test
  void testVerifyObjectMatching_noneMatchSuccess() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"nonematch\"";

    iut.verifyObjectMatching(null, singletonList(etag), s3ObjectMetadata);
  }

  @Test
  void testVerifyObjectMatching_noneMatchWildcard() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"someetag\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(null, Arrays.asList(etag, WILDCARD_ETAG), s3ObjectMetadata)
    ).isEqualTo(NOT_MODIFIED);
  }

  @Test
  void testVerifyObjectMatching_noneMatchFailure() {
    var key = "key";
    var s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key);
    var etag = "\"someetag\"";

    assertThatThrownBy(() ->
        iut.verifyObjectMatching(null, singletonList(etag), s3ObjectMetadata)
    ).isEqualTo(NOT_MODIFIED);
  }

  @Test
  void testVerifyObjectLockConfiguration_failure() {
    var bucketName = "bucket";
    var prefix = "";
    var key = "key";
    givenBucketWithContents(bucketName, prefix, singletonList(givenS3Object(key)));
    assertThatThrownBy(() -> iut.verifyObjectLockConfiguration(bucketName, key))
        .isEqualTo(NOT_FOUND_OBJECT_LOCK);
  }

  @Test
  void testVerifyObjectExists_success() {
    var bucketName = "bucket";
    var prefix = "";
    var key = "key";
    givenBucketWithContents(bucketName, prefix, singletonList(givenS3Object(key)));
    var s3ObjectMetadata = iut.verifyObjectExists(bucketName, key);
    assertThat(s3ObjectMetadata.key()).isEqualTo(key);
  }

  @Test
  void testVerifyObjectExists_failure() {
    var bucketName = "bucket";
    var key = "key";
    givenBucket(bucketName);
    assertThatThrownBy(() -> iut.verifyObjectExists(bucketName, key)).isEqualTo(NO_SUCH_KEY);
  }

  @Test
  void test_toTempFile() throws IOException {
    File file = new File("src/test/resources/sampleFile_large.txt");
    Path tempFile = toTempFile(file.toPath(), Algorithm.SHA256, X_AMZ_CHECKSUM_SHA256);
    var tempFileAndChecksum = iut.toTempFile(Files.newInputStream(tempFile),
        new HttpHeaders(new MultiValueMapAdapter<>(
            Map.of(X_AMZ_SDK_CHECKSUM_ALGORITHM, List.of(ChecksumAlgorithm.SHA256.toString()),
                HttpHeaders.CONTENT_ENCODING, List.of(AWS_CHUNKED),
                X_AMZ_TRAILER, List.of(X_AMZ_CHECKSUM_SHA256))
        )));
    assertThat(tempFileAndChecksum.getLeft().getFileName().toString()).contains("toTempFile");
    assertThat(tempFileAndChecksum.getRight())
        .contains("Y8S4/uAGut7vjdFZQjLKZ7P28V9EPWb4BIoeniuM0mY=");
  }

  private Path toTempFile(Path path, Algorithm algorithm, String header) throws IOException {
    AwsUnsignedChunkedEncodingInputStream.Builder builder = AwsUnsignedChunkedEncodingInputStream
        .builder()
        .inputStream(Files.newInputStream(path));
    if (algorithm != null) {
      builder.sdkChecksum(SdkChecksum.forAlgorithm(algorithm));
    }
    Path tempFile = Files.createTempFile("temp", "");
    try (InputStream chunkedEncodingInputStream = builder
        .checksumHeaderForTrailer(header)
        //force chunks in the inputstream
        .awsChunkedEncodingConfig(AwsChunkedEncodingConfig.builder().chunkSize(4000).build())
        .build();
        OutputStream outputStream = Files.newOutputStream(tempFile)) {
      chunkedEncodingInputStream.transferTo(outputStream);
    }
    return tempFile;
  }
}
