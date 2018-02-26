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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test the application using the AmazonS3 client.
 */
@SuppressWarnings("javadoc")
public class AmazonClientUploadIT extends S3TestBase{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Verify that buckets can be created and listed
   */
  @Test
  public void shouldCreateBucketAndListAllBuckets() {
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    final Date creationDate = new Date((System.currentTimeMillis() / 1000) * 1000 - 60000);

    final Bucket bucket = s3Client.createBucket(BUCKET_NAME);
    assertThat(
        String.format("Bucket name should match '%s'!", BUCKET_NAME), bucket.getName(),
        equalTo(BUCKET_NAME));

    final List<Bucket> buckets =
        s3Client.listBuckets().stream().filter(b -> BUCKET_NAME.equals(b.getName()))
            .collect(Collectors.toList());

    assertThat("Expecting one bucket", buckets, hasSize(1));
    final Bucket createdBucket = buckets.get(0);

    assertThat(createdBucket.getCreationDate(), greaterThanOrEqualTo(creationDate));
    final Owner bucketOwner = createdBucket.getOwner();
    assertThat(bucketOwner.getDisplayName(), equalTo("s3-mock-file-store"));
    assertThat(bucketOwner.getId(), equalTo("123"));
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  public void defaultBucketsGotCreated() {
    final List<Bucket> buckets = s3Client.listBuckets();
    final Set<String> bucketNames = buckets.stream().map(Bucket::getName)
        .filter(INITIAL_BUCKET_NAMES::contains).collect(Collectors.toSet());

    assertThat("Not all default Buckets got created", bucketNames,
        is(equalTo(new HashSet<>(INITIAL_BUCKET_NAMES))));

  }

  /**
   * Verifies {@link AmazonS3#doesObjectExist}.
   */
  @Test
  public void putObjectWhereKeyContainsPathFragments() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    final boolean objectExist = s3Client.doesObjectExist(BUCKET_NAME, UPLOAD_FILE_NAME);
    assertThat(objectExist, is(true));
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadObject() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    final InputStream uploadFileIS = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIS);
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    uploadFileIS.close();
    s3Object.close();

    assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
        is(equalTo(downloadedHash)));
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadStream() throws Exception {
    s3Client.createBucket(BUCKET_NAME);
    final String resourceId = UUID.randomUUID().toString();

    final byte[] resource = new byte[] {1, 2, 3, 4, 5};
    final ByteArrayInputStream bais = new ByteArrayInputStream(resource);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(resource.length);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, resourceId, bais, objectMetadata);

    final TransferManager tm = createDefaultTransferManager();
    final Upload upload = tm.upload(putObjectRequest);

    upload.waitForUploadResult();

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, resourceId);

    final String uploadHash = HashUtil.getDigest(new ByteArrayInputStream(resource));
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    s3Object.close();

    assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
        is(equalTo(downloadedHash)));
  }

  /**
   * Tests if Object can be uploaded with KMS
   */
  @Test
  public void shouldUploadWithEncryption() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String objectKey = UPLOAD_FILE_NAME;
    s3Client.createBucket(BUCKET_NAME);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, uploadFile);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_ENC_KEYREF));

    s3Client.putObject(putObjectRequest);

    final GetObjectMetadataRequest getObjectMetadataRequest =
        new GetObjectMetadataRequest(BUCKET_NAME, objectKey);

    final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(getObjectMetadataRequest);

    assertThat(objectMetadata.getContentLength(), is(uploadFile.length()));
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key
   */
  @Test
  public void shouldNotUploadWithWrongEncryptionKey() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
    s3Client.putObject(putObjectRequest);
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key
   */
  @Test
  public void shouldNotUploadStreamingWithWrongEncryptionKey() {
    final byte[] bytes = UPLOAD_FILE_NAME.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);
    final String objectKey = UUID.randomUUID().toString();
    s3Client.createBucket(BUCKET_NAME);
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, stream, metadata);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
    s3Client.putObject(putObjectRequest);
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new
   * bucket; compares checksums
   * of original and copied object
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  public void shouldCopyObject() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    s3Client.copyObject(copyObjectRequest);

    final com.amazonaws.services.s3.model.S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat("Sourcefile and copied File should have same Hashes",
        copiedHash,
        is(equalTo(putObjectResult.getETag())));
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new
   * bucket; compares checksums
   * of original and copied object
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  public void shouldCopyObjectEncrypted() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;

    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    copyObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_ENC_KEYREF));

    final CopyObjectResult copyObjectResult = s3Client.copyObject(copyObjectRequest);

    final ObjectMetadata metadata =
        s3Client.getObjectMetadata(destinationBucketName, destinationKey);

    final InputStream uploadFileIS = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(TEST_ENC_KEYREF, uploadFileIS);
    assertThat("ETag should match", copyObjectResult.getETag(), is(uploadHash));
    assertThat("Files should have the same length", metadata.getContentLength(),
        is(uploadFile.length()));
  }

  /**
   * Tests that an object wont be copied with wrong encryption Key
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  public void shouldNotObjectCopyWithWrongEncryptionKey() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf" + sourceKey;

    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    copyObjectRequest
        .setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
    s3Client.copyObject(copyObjectRequest);
  }

  /**
   * Creates a bucket and checks if it exists using {@link AmazonS3Client#doesBucketExist(String)}.
   */
  @Test
  public void bucketShouldExist() {
    s3Client.createBucket(BUCKET_NAME);

    final Boolean doesBucketExist = s3Client.doesBucketExist(BUCKET_NAME);

    assertThat(String.format("The previously created bucket, '%s', should exist!", BUCKET_NAME),
        doesBucketExist,
        is(true));
  }

  /**
   * Checks if {@link AmazonS3Client#doesBucketExist(String)} is false on a not existing Bucket.
   */
  @Test
  public void bucketShouldNotExist() {
    final Boolean doesBucketExist = s3Client.doesBucketExist(BUCKET_NAME);

    assertThat(String.format("The bucket, '%s', should not exist!", BUCKET_NAME), doesBucketExist,
        is(false));
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  public void shouldGetObjectMetadata() {
    final String nonExistingFileName = "nonExistingFileName";
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile).withMetadata(objectMetadata));
    final ObjectMetadata metadataExisting =
        s3Client.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME);

    assertThat("The ETags should be identically!", metadataExisting.getETag(),
        is(putObjectResult.getETag()));
    assertThat("User metadata should be identically!", metadataExisting.getUserMetadata(),
            is(equalTo(objectMetadata.getUserMetadata())));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 404"));
    s3Client.getObjectMetadata(BUCKET_NAME, nonExistingFileName);
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  public void shouldDeleteObject() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    s3Client.deleteObject(BUCKET_NAME, UPLOAD_FILE_NAME);

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 404"));
    s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
  }

  /**
   * Tests if an object can be deleted
   */
  @Test
  public void shouldBatchDeleteObjects() {
    final File uploadFile1 = new File(UPLOAD_FILE_NAME);
    final File uploadFile2 = new File(UPLOAD_FILE_NAME);
    final File uploadFile3 = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "1_" + UPLOAD_FILE_NAME, uploadFile1));
    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "2_" + UPLOAD_FILE_NAME, uploadFile2));
    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "3_" + UPLOAD_FILE_NAME, uploadFile3));

    final DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(BUCKET_NAME);

    final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
    keys.add(new DeleteObjectsRequest.KeyVersion("1_" + UPLOAD_FILE_NAME));
    keys.add(new DeleteObjectsRequest.KeyVersion("2_" + UPLOAD_FILE_NAME));
    keys.add(new DeleteObjectsRequest.KeyVersion("3_" + UPLOAD_FILE_NAME));

    multiObjectDeleteRequest.setKeys(keys);

    final DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
    assertThat("Response should contain 4 entries",
        delObjRes.getDeletedObjects().size(), is(3));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 404"));

    s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
  }

  /**
   * Tests that a bucket can be deleted
   */
  @Test
  public void shouldDeleteBucket() {
    s3Client.createBucket(BUCKET_NAME);
    s3Client.deleteBucket(BUCKET_NAME);

    final Boolean doesBucketExist = s3Client.doesBucketExist(BUCKET_NAME);
    assertThat("Deleted Bucket should not exist!", doesBucketExist, is(false));
  }

  /**
   * Tests if the list objects can be retrieved.
   */
  @Test
  public void shouldGetObjectListing() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    final ObjectListing objectListingResult =
        s3Client.listObjects(BUCKET_NAME, UPLOAD_FILE_NAME);

    assertThat("ObjectListinig has no S3Objects.",
        objectListingResult.getObjectSummaries().size(),
        is(greaterThan(0)));
    assertThat("The Name of the first S3ObjectSummary item has not expected the key name.",
        objectListingResult.getObjectSummaries().get(0).getKey(),
        is(UPLOAD_FILE_NAME));
  }

  /**
   * Tests if an object can be uploaded asynchronously
   *
   * @throws Exception not expected
   */
  @Test
  public void shouldUploadInParallel() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();
    final Upload upload =
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey(), equalTo(UPLOAD_FILE_NAME));

    final S3Object getResult = s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
    assertThat(getResult.getKey(), equalTo(UPLOAD_FILE_NAME));
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   *
   * @throws Exception not expected
   */
  @Test
  public void shouldListMultipartUploads() throws Exception {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads(), is(empty()));

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    final MultipartUploadListing listing =
        s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME));
    assertThat(listing.getMultipartUploads(), is(not(empty())));
    assertThat(listing.getBucketName(), equalTo(BUCKET_NAME));
    assertThat(listing.getMultipartUploads(), hasSize(1));
    final MultipartUpload upload = listing.getMultipartUploads().get(0);
    assertThat(upload.getUploadId(), equalTo(uploadId));
    assertThat(upload.getKey(), equalTo(UPLOAD_FILE_NAME));
  }

  /**
   * Tests if a multipart upload can be aborted.
   *
   * @throws Exception not expected
   */
  @Test
  public void shouldAbortMultipartUpload() throws Exception {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads(), is(empty()));

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads(), is(not(empty())));

    s3Client.abortMultipartUpload(
        new AbortMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId));

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
        .getMultipartUploads(), is(empty()));
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  public void checkRangeDownloads() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();
    final Upload upload =
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    upload.waitForUploadResult();

    final File downloadFile = File.createTempFile(UUID.randomUUID().toString(), null);
    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(1, 2),
            downloadFile)
        .waitForCompletion();
    assertThat("Invalid file length", downloadFile.length(), is(2L));

    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(0, 1000),
            downloadFile)
        .waitForCompletion();
    assertThat("Invalid file length", downloadFile.length(), is(uploadFile.length()));
  }

  /**
   * Verifies multipart copy.
   *
   * @throws InterruptedException
   */
  @Test
  public void multipartCopy() throws InterruptedException, IOException, NoSuchAlgorithmException {
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
    final Copy copy =
        transferManager.copy(sourceBucket.getName(), assumedSourceKey, targetBucket.getName(),
            assumedDestinationKey);
    final CopyResult copyResult = copy.waitForCopyResult();
    assertThat(copyResult.getDestinationKey(), is(assumedDestinationKey));

    final S3Object copiedObject = s3Client.getObject(targetBucket.getName(), assumedDestinationKey);

    assertThat("Hashes for source and target S3Object do not match.",
        HashUtil.getDigest(copiedObject.getObjectContent()) + "-1",
        is(uploadResult.getETag()));
  }
}
