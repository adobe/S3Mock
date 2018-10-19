/*
 *  Copyright 2017-2018 Adobe.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies S3 Mocks Error Responses.
 */
public class ErrorResponsesIT extends S3TestBase {

  private static final String NO_SUCH_BUCKET = "Status Code: 404; Error Code: NoSuchBucket";
  private static final String NO_SUCH_KEY = "Status Code: 404; Error Code: NoSuchKey";
  private static final String STATUS_CODE_404 = "Status Code: 404";

  /**
   * Verifies that {@code NoSuchBucket} is returned in Error Response if {@code putObject}
   * references a non existing Bucket.
   */
  @Test
  public void putObjectOnNonExistingBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    AmazonServiceException e = Assertions.assertThrows(AmazonServiceException.class, () -> {
      s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Verifies that {@code NoSuchBucket} is returned in Error Response if {@code putObject}
   * references a non existing Bucket.
   */
  @Test
  public void putObjectEncryptedOnNonExistingBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_ENC_KEYREF));

    AmazonServiceException e = Assertions.assertThrows(AmazonServiceException.class, () -> {
      s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Verifies that {@code NoSuchBucket} is returned in Error Response if {@code copyObject}
   * references a non existing destination Bucket.
   */
  @Test
  public void copyObjectToNonExistingDestinationBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);

    AmazonServiceException e = Assertions.assertThrows(AmazonServiceException.class, () -> {
      s3Client.copyObject(copyObjectRequest);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Verifies that {@code NoSuchBucket} is returned in Error Response if {@code copyObject}
   * encrypted references a non existing destination Bucket.
   */
  @Test
  public void copyObjectEncryptedToNonExistingDestinationBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    copyObjectRequest.setSSEAwsKeyManagementParams(
        new SSEAwsKeyManagementParams(TEST_ENC_KEYREF));

    AmazonServiceException e = Assertions.assertThrows(AmazonServiceException.class, () -> {
      s3Client.copyObject(copyObjectRequest);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  public void getObjectMetadataWithNonExistingBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
        .withMetadata(objectMetadata));

    AmazonServiceException e = Assertions.assertThrows(AmazonServiceException.class, () -> {
      s3Client.getObjectMetadata(UUID.randomUUID().toString(), UPLOAD_FILE_NAME);
    });

    assertThat(e.getMessage(), containsString(STATUS_CODE_404));
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  public void deleteFromNonExistingBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.deleteObject(UUID.randomUUID().toString(), UPLOAD_FILE_NAME);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if deleting an Object returns {@code 204 No Content} even of the given key does not
   * exist.
   */
  @Test
  public void deleteNonExistingObject() {
    s3Client.createBucket(BUCKET_NAME);

    s3Client.deleteObject(BUCKET_NAME, UUID.randomUUID().toString());
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  public void batchDeleteObjectsFromNonExistingBucket() {
    final File uploadFile1 = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "1_" + UPLOAD_FILE_NAME, uploadFile1));

    final DeleteObjectsRequest multiObjectDeleteRequest =
        new DeleteObjectsRequest(UUID.randomUUID().toString());

    final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
    keys.add(new DeleteObjectsRequest.KeyVersion("1_" + UPLOAD_FILE_NAME));

    multiObjectDeleteRequest.setKeys(keys);

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.deleteObjects(multiObjectDeleteRequest);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests that a bucket can be deleted.
   */
  @Test
  public void deleteNonExistingBucket() {
    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.deleteBucket(BUCKET_NAME);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if the list objects can be retrieved.
   */
  @Test
  public void listObjectsFromNonExistingBucket() {
    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.listObjects(UUID.randomUUID().toString(), UPLOAD_FILE_NAME);
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if an object can be uploaded asynchronously.
   *
   * @throws Exception not expected
   */
  @Test
  public void uploadParallelToNonExistingBucket() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      final Upload upload = transferManager.upload(
              new PutObjectRequest(UUID.randomUUID().toString(), UPLOAD_FILE_NAME, uploadFile));
      upload.waitForUploadResult();
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  public void multipartUploadsToNonExistingBucket() {
    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.initiateMultipartUpload(
              new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  public void listMultipartUploadsFromNonExistingBucket() {
    s3Client.createBucket(BUCKET_NAME);

    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.listMultipartUploads(new ListMultipartUploadsRequest(UUID.randomUUID().toString()));
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  public void abortMultipartUploadInNonExistingBucket() {
    s3Client.createBucket(BUCKET_NAME);

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads(), is(not(empty())));

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.abortMultipartUpload(
              new AbortMultipartUploadRequest(UUID.randomUUID().toString(), UPLOAD_FILE_NAME, uploadId));
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  public void rangeDownloadsFromNonExistingBucket() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();
    final Upload upload =
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    upload.waitForUploadResult();

    final File downloadFile = File.createTempFile(UUID.randomUUID().toString(), null);

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      transferManager.download(
              new GetObjectRequest(UUID.randomUUID().toString(), UPLOAD_FILE_NAME).withRange(1, 2),
              downloadFile).waitForCompletion();
    });

    assertThat(e.getMessage(), containsString(STATUS_CODE_404));
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  public void rangeDownloadsFromNonExistingObject() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();
    final Upload upload =
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    upload.waitForUploadResult();

    final File downloadFile = File.createTempFile(UUID.randomUUID().toString(), null);

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      transferManager.download(
              new GetObjectRequest(BUCKET_NAME, UUID.randomUUID().toString()).withRange(1, 2),
              downloadFile).waitForCompletion();
    });

    assertThat(e.getMessage(), containsString(STATUS_CODE_404));
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  public void multipartCopyToNonExistingBucket() throws InterruptedException {
    final int contentLen = 3 * _1MB;

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(contentLen);

    final String assumedSourceKey = UUID.randomUUID().toString();

    final Bucket sourceBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final TransferManager transferManager = createTransferManager(_2MB, _1MB, _2MB, _1MB);

    final InputStream sourceInputStream = randomInputStream(contentLen);
    final Upload upload = transferManager
        .upload(sourceBucket.getName(), assumedSourceKey,
            sourceInputStream, objectMetadata);

    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey(), is(assumedSourceKey));

    final String assumedDestinationKey = UUID.randomUUID().toString();

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      transferManager.copy(sourceBucket.getName(), assumedSourceKey, UUID.randomUUID().toString(),
              assumedDestinationKey).waitForCopyResult();
    });

    assertThat(e.getMessage(), containsString(NO_SUCH_BUCKET));
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  public void multipartCopyNonExistingObject() throws InterruptedException {
    final int contentLen = 3 * _1MB;

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(contentLen);

    final String assumedSourceKey = UUID.randomUUID().toString();

    final Bucket sourceBucket = s3Client.createBucket(UUID.randomUUID().toString());
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final TransferManager transferManager = createTransferManager(_2MB, _1MB, _2MB, _1MB);

    final InputStream sourceInputStream = randomInputStream(contentLen);
    final Upload upload = transferManager
        .upload(sourceBucket.getName(), assumedSourceKey,
            sourceInputStream, objectMetadata);

    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey(), is(assumedSourceKey));

    final String assumedDestinationKey = UUID.randomUUID().toString();

    AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      transferManager.copy(sourceBucket.getName(), UUID.randomUUID().toString(),
              targetBucket.getName(), assumedDestinationKey).waitForCopyResult();
    });

    assertThat(e.getMessage(), containsString(STATUS_CODE_404));
  }
}
