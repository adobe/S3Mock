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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_NUMBER;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static com.adobe.testing.s3mock.service.MultipartService.MINIMUM_PART_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.MultipartStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(classes = {ServiceConfiguration.class})
@MockBean({BucketService.class, ObjectService.class, ObjectStore.class})
class MultipartServiceTest extends ServiceTestBase {

  @MockBean
  private MultipartStore multipartStore;

  @Autowired
  private MultipartService iut;

  @Test
  void testVerifyPartNumberLimits_success() {
    String partNumber = "1";
    iut.verifyPartNumberLimits(partNumber);
  }

  @Test
  void testVerifyPartNumberLimits_tooSmallFailure() {
    String partNumber = "0";
    assertThatThrownBy(() ->
        iut.verifyPartNumberLimits(partNumber)
    ).isEqualTo(INVALID_PART_NUMBER);
  }

  @Test
  void testVerifyPartNumberLimits_tooLargeFailure() {
    String partNumber = "10001";
    assertThatThrownBy(() ->
        iut.verifyPartNumberLimits(partNumber)
    ).isEqualTo(INVALID_PART_NUMBER);
  }

  @Test
  void testVerifyPartNumberLimits_noNumberFailure() {
    String partNumber = "NOT A NUMBER";
    assertThatThrownBy(() ->
        iut.verifyPartNumberLimits(partNumber)
    ).isEqualTo(INVALID_PART_NUMBER);
  }

  @Test
  void testVerifyMultipartParts_withRequestedParts_success() {
    String bucketName = "bucketName";
    String key = "key";
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    UUID id = bucketMetadata.addKey(key);
    List<Part> parts = givenParts(2, MINIMUM_PART_SIZE);
    List<CompletedPart> requestedParts = from(parts);
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);

    iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts);
  }

  @Test
  void testVerifyMultipartParts_withRequestedParts_wrongPartsFailure() {
    String bucketName = "bucketName";
    String key = "key";
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    UUID id = bucketMetadata.addKey(key);
    List<Part> parts = givenParts(1, 1L);
    List<CompletedPart> requestedParts = List.of(new CompletedPart(1, "1L"));
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);

    assertThatThrownBy(() ->
        iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts)
    ).isEqualTo(INVALID_PART);
  }

  @Test
  void testVerifyMultipartParts_withRequestedParts_wrongPartOrderFailure() {
    String bucketName = "bucketName";
    String key = "key";
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    UUID id = bucketMetadata.addKey(key);
    List<Part> parts = givenParts(2, MINIMUM_PART_SIZE);
    List<CompletedPart> requestedParts = new ArrayList<>(from(parts));
    Collections.reverse(requestedParts);
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);

    assertThatThrownBy(() ->
        iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts)
    ).isEqualTo(INVALID_PART_ORDER);
  }

  private List<CompletedPart> from(List<Part> parts) {
    return parts
        .stream()
        .map(part -> new CompletedPart(part.partNumber(), part.etag()))
        .toList();
  }

  @Test
  void testVerifyMultipartParts_onePart() {
    String bucketName = "bucketName";
    UUID id = UUID.randomUUID();
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    List<Part> parts = givenParts(1, 1L);
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);

    iut.verifyMultipartParts(bucketName, id, uploadId);
  }

  @Test
  void testVerifyMultipartParts_twoParts() {
    String bucketName = "bucketName";
    UUID id = UUID.randomUUID();
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    List<Part> parts = givenParts(2, MINIMUM_PART_SIZE);
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);

    iut.verifyMultipartParts(bucketName, id, uploadId);
  }

  @Test
  void testVerifyMultipartParts_twoPartsFailure() {
    String bucketName = "bucketName";
    UUID id = UUID.randomUUID();
    String uploadId = "uploadId";
    BucketMetadata bucketMetadata = givenBucket(bucketName);
    List<Part> parts = givenParts(2, 1L);
    when(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts);
    assertThatThrownBy(() ->
        iut.verifyMultipartParts(bucketName, id, uploadId)
    ).isEqualTo(ENTITY_TOO_SMALL);
  }

  @Test
  void testVerifyMultipartParts_failure() {
    String uploadId = "uploadId";
    when(multipartStore.getMultipartUpload(uploadId)).thenThrow(new IllegalArgumentException());
    assertThatThrownBy(() ->
        iut.verifyMultipartUploadExists(uploadId)
    ).isEqualTo(NO_SUCH_UPLOAD_MULTIPART);
  }

  @Test
  void testVerifyMultipartUploadExists_success() {
    String uploadId = "uploadId";
    iut.verifyMultipartUploadExists(uploadId);
  }

  @Test
  void testVerifyMultipartUploadExists_failure() {
    String uploadId = "uploadId";
    when(multipartStore.getMultipartUpload(uploadId)).thenThrow(new IllegalArgumentException());
    assertThatThrownBy(() ->
        iut.verifyMultipartUploadExists(uploadId)
    ).isEqualTo(NO_SUCH_UPLOAD_MULTIPART);
  }

}
