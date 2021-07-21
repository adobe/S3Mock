/*
 *  Copyright 2017-2021 Adobe.
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

package com.adobe.testing.s3mock.its;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

public class MultiPartUploadIT extends S3TestBase {

  private static final Random random = new Random();
  private static final int BUFFER_SIZE = 128 * 1024;

  /**
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  void shouldPassUserMetadataWithMultipartUploads() {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    final UploadPartResult uploadPartResult = s3Client.uploadPart(new UploadPartRequest()
        .withBucketName(initiateMultipartUploadResult.getBucketName())
        .withKey(initiateMultipartUploadResult.getKey())
        .withUploadId(uploadId)
        .withFile(uploadFile)
        .withFileOffset(0)
        .withPartNumber(1)
        .withPartSize(uploadFile.length())
        .withLastPart(true));

    final List<PartETag> partETags = singletonList(uploadPartResult.getPartETag());
    s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(
        initiateMultipartUploadResult.getBucketName(),
        initiateMultipartUploadResult.getKey(),
        initiateMultipartUploadResult.getUploadId(),
        partETags
    ));

    final ObjectMetadata metadataExisting = s3Client.getObjectMetadata(
        initiateMultipartUploadResult.getBucketName(), initiateMultipartUploadResult.getKey()
    );

    assertThat(metadataExisting.getUserMetadata()).as("User metadata should be identical!")
        .isEqualTo(objectMetadata.getUserMetadata());
  }

  @Test
  void shouldInitiateMultipartAndRetrieveParts() throws IOException {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    final String hash = DigestUtils.md5Hex(new FileInputStream(uploadFile));
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();
    final String key = initiateMultipartUploadResult.getKey();

    s3Client.uploadPart(new UploadPartRequest()
        .withBucketName(initiateMultipartUploadResult.getBucketName())
        .withKey(initiateMultipartUploadResult.getKey())
        .withUploadId(uploadId)
        .withFile(uploadFile)
        .withFileOffset(0)
        .withPartNumber(1)
        .withPartSize(uploadFile.length())
        .withLastPart(true));

    final ListPartsRequest listPartsRequest = new ListPartsRequest(
        BUCKET_NAME,
        key,
        uploadId
    );
    final PartListing partListing = s3Client.listParts(listPartsRequest);

    assertThat(partListing.getParts()).as("Part listing should be 1").hasSize(1);
    final PartSummary partSummary = partListing.getParts().get(0);

    assertThat(partSummary.getETag()).as("Etag should match").isEqualTo(hash);
    assertThat(partSummary.getPartNumber()).as("Part number should match").isEqualTo(1);
    assertThat(partSummary.getLastModified()).as("LastModified should be valid date")
        .isExactlyInstanceOf(Date.class);
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  void shouldListMultipartUploads() {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    final MultipartUploadListing listing =
        s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME));
    assertThat(listing.getMultipartUploads()).isNotEmpty();
    assertThat(listing.getBucketName()).isEqualTo(BUCKET_NAME);
    assertThat(listing.getMultipartUploads()).hasSize(1);
    final MultipartUpload upload = listing.getMultipartUploads().get(0);
    assertThat(upload.getUploadId()).isEqualTo(uploadId);
    assertThat(upload.getKey()).isEqualTo(UPLOAD_FILE_NAME);
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed with prefix filtering.
   */
  @Test
  void shouldListMultipartUploadsWithPrefix() {
    s3Client.createBucket(BUCKET_NAME);
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, "key1"));
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, "key2"));

    final ListMultipartUploadsRequest listMultipartUploadsRequest =
        new ListMultipartUploadsRequest(BUCKET_NAME);
    listMultipartUploadsRequest.setPrefix("key2");
    final MultipartUploadListing listing
        = s3Client.listMultipartUploads(listMultipartUploadsRequest);

    assertThat(listing.getMultipartUploads()).hasSize(1);
    assertThat(listing.getMultipartUploads().get(0).getKey()).isEqualTo("key2");
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  void shouldAbortMultipartUpload() {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isNotEmpty();

    s3Client.abortMultipartUpload(
        new AbortMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId));

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();
  }

  /**
   * Tests if the parts specified in CompleteUploadRequest are adhered
   * irrespective of the number of parts uploaded before.
   */
  @Test
  void shouldAdherePartsInCompleteMultipartUploadRequest() throws IOException {
    s3Client.createBucket(BUCKET_NAME);
    final String key = UUID.randomUUID().toString();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();

    // Initiate upload
    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, key));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    // Upload 3 parts
    byte[] randomBytes1 = createRandomBytes();
    PartETag partETag1 = s3Client
        .uploadPart(getUploadPartRequest(key, uploadId)
            .withPartNumber(1)
            .withPartSize(randomBytes1.length)
            .withInputStream(new ByteArrayInputStream(randomBytes1)))
        .getPartETag();

    byte[] randomBytes2 = createRandomBytes();
    PartETag partETag2 = s3Client
        .uploadPart(getUploadPartRequest(key, uploadId)
            .withPartNumber(2)
            .withPartSize(randomBytes2.length)
            .withInputStream(new ByteArrayInputStream(randomBytes2)))
        .getPartETag();

    byte[] randomBytes3 = createRandomBytes();
    PartETag partETag3 = s3Client
        .uploadPart(getUploadPartRequest(key, uploadId)
            .withPartNumber(3)
            .withPartSize(randomBytes3.length)
            .withInputStream(new ByteArrayInputStream(randomBytes3)))
        .getPartETag();

    // Adding to parts list only 1st and 3rd part
    List<PartETag> parts = new ArrayList<>();
    parts.add(partETag1);
    parts.add(partETag3);

    // Try to complete with these parts
    final CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(BUCKET_NAME, key, uploadId, parts));

    // Verify only 1st and 3rd counts
    S3Object object = s3Client.getObject(BUCKET_NAME, key);

    final byte[] allMd5s = ArrayUtils.addAll(
        DigestUtils.md5(randomBytes1),
        DigestUtils.md5(randomBytes3)
    );

    // verify special etag
    assertThat(result.getETag()).as("Special etag doesn't match.")
        .isEqualTo(DigestUtils.md5Hex(allMd5s) + "-2");

    // verify content size
    assertThat(object.getObjectMetadata().getContentLength()).as("Content length doesn't match")
        .isEqualTo((long) randomBytes1.length + randomBytes3.length);

    // verify contents
    assertThat(readStreamIntoByteArray(object.getObjectContent())).as(
            "Object contents doesn't match")
        .isEqualTo(concatByteArrays(randomBytes1, randomBytes3));

  }

  private UploadPartRequest getUploadPartRequest(String key, String uploadId) {
    return new UploadPartRequest()
        .withBucketName(BUCKET_NAME)
        .withKey(key)
        .withUploadId(uploadId);
  }

  private byte[] createRandomBytes() {
    int size = 5 * 1024 * 1024 + random.nextInt(1024 * 1024);
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return bytes;
  }

  private byte[] readStreamIntoByteArray(InputStream inputStream) throws IOException {
    try (InputStream in = inputStream) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SIZE);

      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead = -1;
      while ((bytesRead = in.read(buffer)) != -1) {
        baos.write(buffer, 0, bytesRead);
      }
      baos.flush();

      return baos.toByteArray();
    }
  }

  private byte[] concatByteArrays(byte[] arr1, byte[] arr2) {
    byte[] result = new byte[arr1.length + arr2.length];

    System.arraycopy(arr1, 0, result, 0, arr1.length);
    System.arraycopy(arr2, 0, result, arr1.length, arr2.length);

    return result;
  }
}
