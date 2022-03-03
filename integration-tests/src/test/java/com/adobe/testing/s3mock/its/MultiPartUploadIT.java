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

package com.adobe.testing.s3mock.its;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
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
import java.util.Collections;
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
  private static final String BUCKET_NAME_2 = "testbucket2";

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
   * Tests if multipart uploads are stored and can be retrieved by bucket.
   */
  @Test
  void shouldListMultipartUploadsWithBucket() {
    // create multipart upload 1
    s3Client.createBucket(BUCKET_NAME);
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, "key1"));
    // create multipart upload 2
    s3Client.createBucket(BUCKET_NAME_2);
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME_2, "key2"));

    // assert multipart upload 1
    final ListMultipartUploadsRequest listMultipartUploadsRequest1 =
        new ListMultipartUploadsRequest(BUCKET_NAME);
    final MultipartUploadListing listing1
        = s3Client.listMultipartUploads(listMultipartUploadsRequest1);

    assertThat(listing1.getMultipartUploads()).hasSize(1);
    assertThat(listing1.getMultipartUploads().get(0).getKey()).isEqualTo("key1");

    // assert multipart upload 2
    final ListMultipartUploadsRequest listMultipartUploadsRequest2 =
        new ListMultipartUploadsRequest(BUCKET_NAME_2);
    final MultipartUploadListing listing2
        = s3Client.listMultipartUploads(listMultipartUploadsRequest2);

    assertThat(listing2.getMultipartUploads()).hasSize(1);
    assertThat(listing2.getMultipartUploads().get(0).getKey()).isEqualTo("key2");
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

    byte[] randomBytes = createRandomBytes();
    PartETag partETag = uploadPart(UPLOAD_FILE_NAME, uploadId, 1, randomBytes);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isNotEmpty();

    List<PartSummary> partsBeforeComplete =
        s3Client.listParts(new ListPartsRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId))
            .getParts();
    assertThat(partsBeforeComplete).hasSize(1);
    assertThat(partsBeforeComplete.get(0).getETag()).isEqualTo(partETag.getETag());

    s3Client.abortMultipartUpload(
        new AbortMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId));

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();

    assertThat(s3Client.listParts(new ListPartsRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId))
        .getParts()).hasSize(0);
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
    PartETag partETag1 = uploadPart(key, uploadId, 1, randomBytes1);

    byte[] randomBytes2 = createRandomBytes();
    PartETag partETag2 = uploadPart(key, uploadId, 2, randomBytes2);

    byte[] randomBytes3 = createRandomBytes();
    PartETag partETag3 = uploadPart(key, uploadId, 3, randomBytes3);

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

  /**
   * Tests that uploaded parts can be listed regardless if the MultipartUpload was completed or
   * aborted.
   */
  @Test
  void shouldListPartsOnCompleteOrAbort() {
    s3Client.createBucket(BUCKET_NAME);
    final String key = UUID.randomUUID().toString();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads()).isEmpty();

    // Initiate upload
    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, key));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    // Upload part
    byte[] randomBytes = createRandomBytes();
    PartETag partETag = uploadPart(key, uploadId, 1, randomBytes);

    // List parts, make sure we find part 1
    List<PartSummary> partsBeforeComplete =
        s3Client.listParts(new ListPartsRequest(BUCKET_NAME, key, uploadId))
            .getParts();
    assertThat(partsBeforeComplete).hasSize(1);
    assertThat(partsBeforeComplete.get(0).getETag()).isEqualTo(partETag.getETag());

    // Complete
    final CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(BUCKET_NAME, key, uploadId,
            Collections.singletonList(partETag)));

    // List parts, make sure we find no parts
    assertThat(s3Client.listParts(new ListPartsRequest(BUCKET_NAME, key, uploadId))
        .getParts()).hasSize(0);
  }

  /**
   * Upload two objects, copy as parts without length, complete multipart.
   */
  @Test
  void shouldCopyPartsAndComplete() throws IOException {
    //Initiate upload in BUCKET_NAME_2
    s3Client.createBucket(BUCKET_NAME_2);
    String multipartUploadKey = UUID.randomUUID().toString();
    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME_2, multipartUploadKey));
    final String uploadId = initiateMultipartUploadResult.getUploadId();
    List<PartETag> parts = new ArrayList<>();

    //bucket for test data
    s3Client.createBucket(BUCKET_NAME);

    //create two objects, initiate copy part with full object length
    String[] sourceKeys = {UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    List<byte[]> allRandomBytes = new ArrayList<>();
    for (int i = 0; i < sourceKeys.length; i++) {
      String key = sourceKeys[i];
      int partNumber = i + 1;
      byte[] randomBytes = createRandomBytes();
      ObjectMetadata metadata1 = new ObjectMetadata();
      metadata1.setContentLength(randomBytes.length);
      PutObjectResult putObjectResult = //ignore result
          s3Client.putObject(
              new PutObjectRequest(BUCKET_NAME, key, new ByteArrayInputStream(randomBytes),
                  metadata1));
      CopyPartRequest request = new CopyPartRequest()
          .withPartNumber(partNumber)
          .withUploadId(uploadId)
          .withDestinationBucketName(BUCKET_NAME_2)
          .withDestinationKey(multipartUploadKey)
          .withSourceKey(key)
          .withSourceBucketName(BUCKET_NAME);
      CopyPartResult result = s3Client.copyPart(request);
      String etag = result.getETag();
      PartETag partETag = new PartETag(partNumber, etag);
      parts.add(partETag);
      allRandomBytes.add(randomBytes);
    }

    assertThat(allRandomBytes).hasSize(2);

    // Complete with parts
    final CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(BUCKET_NAME_2, multipartUploadKey, uploadId, parts));

    // Verify parts
    S3Object object = s3Client.getObject(BUCKET_NAME_2, multipartUploadKey);

    byte[] allMd5s = ArrayUtils.addAll(
        DigestUtils.md5(allRandomBytes.get(0)),
        DigestUtils.md5(allRandomBytes.get(1))
    );

    // verify etag
    assertThat(result.getETag()).as("etag doesn't match.")
        .isEqualTo(DigestUtils.md5Hex(allMd5s) + "-2");

    // verify content size
    assertThat(object.getObjectMetadata().getContentLength()).as("Content length doesn't match")
        .isEqualTo((long) allRandomBytes.get(0).length + allRandomBytes.get(1).length);

    // verify contents
    assertThat(readStreamIntoByteArray(object.getObjectContent())).as(
            "Object contents doesn't match")
        .isEqualTo(concatByteArrays(allRandomBytes.get(0), allRandomBytes.get(1)));
  }

  /**
   * Puts an Object; Copies part of that object to a new bucket;
   * Requests parts for the uploadId; compares etag of upload response and parts list.
   */
  @Test
  void shouldCopyObjectPart() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(destinationBucketName, destinationKey,
                objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    CopyPartRequest copyPartRequest = new CopyPartRequest();
    copyPartRequest.setDestinationBucketName(destinationBucketName);
    copyPartRequest.setUploadId(uploadId);
    copyPartRequest.setDestinationKey(destinationKey);
    copyPartRequest.setSourceBucketName(BUCKET_NAME);
    copyPartRequest.setSourceKey(sourceKey);
    copyPartRequest.setFirstByte(0L);
    copyPartRequest.setLastByte(putObjectResult.getMetadata().getContentLength());

    CopyPartResult copyPartResult = s3Client.copyPart(copyPartRequest);

    PartListing partListing =
        s3Client.listParts(new ListPartsRequest(initiateMultipartUploadResult.getBucketName(),
            initiateMultipartUploadResult.getKey(),
            initiateMultipartUploadResult.getUploadId()));

    assertThat(partListing.getParts()).hasSize(1);
    assertThat(partListing.getParts().get(0).getETag()).isEqualTo(copyPartResult.getETag());
  }

  /**
   * Tries to copy part of an non-existing object to a new bucket.
   */
  @Test
  void shouldThrowNoSuchKeyOnCopyObjectPartForNonExistingKey() {
    final String sourceKey = "NON_EXISTENT_KEY";
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(destinationBucketName, destinationKey,
                objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    CopyPartRequest copyPartRequest = new CopyPartRequest();
    copyPartRequest.setDestinationBucketName(destinationBucketName);
    copyPartRequest.setUploadId(uploadId);
    copyPartRequest.setDestinationKey(destinationKey);
    copyPartRequest.setSourceBucketName(BUCKET_NAME);
    copyPartRequest.setSourceKey(sourceKey);
    copyPartRequest.setFirstByte(0L);
    copyPartRequest.setLastByte(5L);

    assertThatThrownBy(() -> s3Client.copyPart(copyPartRequest))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 404; Error Code: NoSuchKey");
  }

  private PartETag uploadPart(String key, String uploadId, int partNumber, byte[] randomBytes) {
    return s3Client
        .uploadPart(createUploadPartRequest(key, uploadId)
            .withPartNumber(partNumber)
            .withPartSize(randomBytes.length)
            .withInputStream(new ByteArrayInputStream(randomBytes)))
        .getPartETag();
  }

  private UploadPartRequest createUploadPartRequest(String key, String uploadId) {
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
