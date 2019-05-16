/*
 *  Copyright 2017-2019 Adobe.
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
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.adobe.testing.s3mock.util.EtagInputStream;
import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test the application using the AmazonS3 client.
 */
public class AmazonClientUploadIT extends S3TestBase {
  /**
   * Verify that buckets can be created and listed.
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
            .collect(toList());

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
   * Stores files in a previously created bucket. List files using ListObjectsV2Request
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndListV2Objects() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME,
        uploadFile.getName(), uploadFile));
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME,
        uploadFile.getName() + "copy1", uploadFile));
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME,
        uploadFile.getName() + "copy2", uploadFile));

    final ListObjectsV2Request listReq = new ListObjectsV2Request()
        .withBucketName(BUCKET_NAME)
        .withMaxKeys(3);
    final ListObjectsV2Result listResult = s3Client.listObjectsV2(listReq);
    assertThat(listResult.getKeyCount(), is(3));
    for (final S3ObjectSummary objectSummary : listResult.getObjectSummaries()) {
      assertThat(objectSummary.getKey(), containsString(uploadFile.getName()));
      final S3Object s3Object = s3Client.getObject(BUCKET_NAME, objectSummary.getKey());
      final InputStream uploadFileIs = new FileInputStream(uploadFile);
      final String uploadHash = HashUtil.getDigest(uploadFileIs);
      final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
      uploadFileIs.close();
      s3Object.close();
      assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
          is(equalTo(downloadedHash)));
    }
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

    verifyObjectContent(uploadFile, s3Object);
  }

  /**
   * Uses weird, but valid characters in the key used to store an object.
   *
   * @see #shouldUploadAndDownloadObject()
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldTolerateWeirdCharactersInObjectKey() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_+.,~|\"':^"
        + "\u1234\uabcd\u0000\u0001"; // non-ascii and unprintable stuff
    final String key = weirdStuff + uploadFile.getName() + weirdStuff;

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, key);

    verifyObjectContent(uploadFile, s3Object);
  }

  private void verifyObjectContent(final File uploadFile, final S3Object s3Object)
      throws FileNotFoundException, NoSuchAlgorithmException, IOException {
    final InputStream uploadFileIs = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIs);
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    uploadFileIs.close();
    s3Object.close();

    assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
        is(equalTo(downloadedHash)));
  }

  /**
   * Uses weird, but valid characters in the key used to store an object. Verifies
   * that ListObject returns the correct object names.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldListWithCorrectObjectNames() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_ .,~|\"':^"
        + "\u1234\uabcd\u0000\u0001"; // non-ascii and unprintable stuff
    final String prefix = "shouldListWithCorrectObjectNames/";
    final String key = prefix + weirdStuff + uploadFile.getName() + weirdStuff;

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ObjectListing listing = s3Client.listObjects(BUCKET_NAME, prefix);
    final List<S3ObjectSummary> summaries = listing.getObjectSummaries();

    assertThat("Must have exactly one match", summaries, hasSize(1));
    assertThat("Object name must match", summaries.get(0).getKey(), equalTo(key));
  }

  /**
   * Same as {@link #shouldListWithCorrectObjectNames()} but for V2 API.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldListV2WithCorrectObjectNames() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_ .,~|\"':^"
        + "\u1234\uabcd\u0000\u0001"; // non-ascii and unprintable stuff
    final String prefix = "shouldListWithCorrectObjectNames/";
    final String key = prefix + weirdStuff + uploadFile.getName() + weirdStuff;

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    // AWS client ListObjects V2 defaults to no encoding whereas V1 defaults to URL
    final ListObjectsV2Request lorv2 = new ListObjectsV2Request();
    lorv2.setBucketName(BUCKET_NAME);
    lorv2.setPrefix(prefix);
    lorv2.setEncodingType("url"); // do use encoding!

    final ListObjectsV2Result listing = s3Client.listObjectsV2(lorv2);
    final List<S3ObjectSummary> summaries = listing.getObjectSummaries();

    assertThat("Must have exactly one match", summaries, hasSize(1));
    assertThat("Object name must match", summaries.get(0).getKey(), equalTo(key));
  }

  /**
   * Uses a key that cannot be represented in XML without encoding. Then lists
   * the objects without encoding, expecting a parse exception and thus verifying
   * that the encoding parameter is honored.
   *
   * <p>This isn't the greatest way to test this functionality, however, there
   * is currently no low-level testing infrastructure in place.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldHonorEncodingType() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String prefix = "shouldHonorEncodingType/";
    final String key = prefix + "\u0000"; // key invalid in XML

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ListObjectsRequest lor = new ListObjectsRequest(BUCKET_NAME, prefix, null, null, null);
    lor.setEncodingType(""); // don't use encoding

    // we expect an SdkClientException wich a message pointing to XML
    // parsing issues.
    assertThat(
        assertThrows(SdkClientException.class, () -> {
          s3Client.listObjects(lor);
        }).getMessage(),
        containsString("Failed to parse XML document")
    );
  }

  /**
   * The same as {@link #shouldHonorEncodingType()} but for V2 API.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldHonorEncodingTypeV2() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String prefix = "shouldHonorEncodingType/";
    final String key = prefix + "\u0000"; // key invalid in XML

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ListObjectsV2Request lorv2 = new ListObjectsV2Request();
    lorv2.setBucketName(BUCKET_NAME);
    lorv2.setPrefix(prefix);
    lorv2.setEncodingType(""); // don't use encoding

    // we expect an SdkClientException wich a message pointing to XML
    // parsing issues.
    assertThat(
        assertThrows(SdkClientException.class, () -> {
          s3Client.listObjectsV2(lorv2);
        }).getMessage(),
        containsString("Failed to parse XML document")
    );
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  public void shouldUploadAndDownloadStream() throws Exception {
    s3Client.createBucket(BUCKET_NAME);
    final String resourceId = randomUUID().toString();
    final String contentEncoding = "gzip";

    final byte[] resource = new byte[] {1, 2, 3, 4, 5};
    final ByteArrayInputStream bais = new ByteArrayInputStream(resource);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(resource.length);
    objectMetadata.setContentEncoding(contentEncoding);

    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, resourceId, bais, objectMetadata);

    final TransferManager tm = createDefaultTransferManager();
    final Upload upload = tm.upload(putObjectRequest);

    upload.waitForUploadResult();

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, resourceId);

    assertThat("Uploaded File should have Encoding-Type set",
        s3Object.getObjectMetadata().getContentEncoding(),
        is(equalTo(contentEncoding)));

    final String uploadHash = HashUtil.getDigest(new ByteArrayInputStream(resource));
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    s3Object.close();

    assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
        is(equalTo(downloadedHash)));
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  public void shouldUploadWithEncryption() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String objectKey = UPLOAD_FILE_NAME;
    s3Client.createBucket(BUCKET_NAME);
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.addUserMetadata("key", "value");
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, uploadFile).withMetadata(metadata);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_ENC_KEYREF));

    s3Client.putObject(putObjectRequest);

    final GetObjectMetadataRequest getObjectMetadataRequest =
        new GetObjectMetadataRequest(BUCKET_NAME, objectKey);

    final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(getObjectMetadataRequest);

    assertThat(objectMetadata.getContentLength(), is(uploadFile.length()));

    assertThat("User metadata should be identical!", objectMetadata.getUserMetadata(),
        is(equalTo(metadata.getUserMetadata())));

  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  public void shouldNotUploadWithWrongEncryptionKey() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.putObject(putObjectRequest);
    });

    assertThat(e.getMessage(),
        containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  public void shouldNotUploadStreamingWithWrongEncryptionKey() {
    final byte[] bytes = UPLOAD_FILE_NAME.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);
    final String objectKey = randomUUID().toString();
    s3Client.createBucket(BUCKET_NAME);
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, stream, metadata);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.putObject(putObjectRequest);
    });

    assertThat(e.getMessage(),
        containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
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

    final S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat("Sourcefile and copied File should have same Hashes",
        copiedHash,
        is(equalTo(putObjectResult.getETag())));
  }

  /**
   * Copy an object to a key needing URL escaping.
   *
   * @see #shouldCopyObject()
   * @throws Exception if an Exception occurs
   */
  @Test
  public void shouldCopyObjectToKeyNeedingEscaping() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/some escape-worthy characters %$@ " + sourceKey;
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    s3Client.copyObject(copyObjectRequest);

    final S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat("Sourcefile and copied File should have same Hashes",
        copiedHash,
        is(equalTo(putObjectResult.getETag())));
  }

  /**
   * Copy an object from a key needing URL escaping.
   *
   * @see #shouldCopyObject()
   * @throws Exception if an Exception occurs
   */
  @Test
  public void shouldCopyObjectFromKeyNeedingEscaping() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = "some escape-worthy characters %$@ " + UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey;
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    s3Client.copyObject(copyObjectRequest);

    final S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat("Sourcefile and copied File should have same Hashes",
        copiedHash,
        is(equalTo(putObjectResult.getETag())));
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
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

    final InputStream uploadFileIs = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(TEST_ENC_KEYREF, uploadFileIs);
    assertThat("ETag should match", copyObjectResult.getETag(), is(uploadHash));
    assertThat("Files should have the same length", metadata.getContentLength(),
        is(uploadFile.length()));
  }

  /**
   * Tests that an object wont be copied with wrong encryption Key.
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

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.copyObject(copyObjectRequest);
    });

    assertThat(e.getMessage(),
        containsString("Status Code: 400; Error Code: KMS.NotFoundException"));
  }

  /**
   * Creates a bucket and checks if it exists using {@link AmazonS3Client#doesBucketExist(String)}.
   */
  @Test
  public void bucketShouldExist() {
    s3Client.createBucket(BUCKET_NAME);

    final Boolean doesBucketExist = s3Client.doesBucketExistV2(BUCKET_NAME);

    assertThat(String.format("The previously created bucket, '%s', should exist!", BUCKET_NAME),
        doesBucketExist,
        is(true));
  }

  /**
   * Checks if {@link AmazonS3Client#doesBucketExistV2(String)} is false on a not existing Bucket.
   */
  @Test
  public void bucketShouldNotExist() {
    final Boolean doesBucketExist = s3Client.doesBucketExistV2(BUCKET_NAME);

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
    objectMetadata.setContentEncoding("gzip");

    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
            .withMetadata(objectMetadata));
    final ObjectMetadata metadataExisting =
        s3Client.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME);

    assertThat("Content-Encoding should be identical!", metadataExisting.getContentEncoding(),
        is(putObjectResult.getMetadata().getContentEncoding()));
    assertThat("The ETags should be identical!", metadataExisting.getETag(),
        is(putObjectResult.getETag()));
    assertThat("User metadata should be identical!", metadataExisting.getUserMetadata(),
        is(equalTo(objectMetadata.getUserMetadata())));

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.getObjectMetadata(BUCKET_NAME, nonExistingFileName);
    });

    assertThat(e.getMessage(), containsString("Status Code: 404"));
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

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
    });

    assertThat(e.getMessage(), containsString("Status Code: 404"));
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  public void shouldBatchDeleteObjects() {
    final File uploadFile1 = new File(UPLOAD_FILE_NAME);
    final File uploadFile2 = new File(UPLOAD_FILE_NAME);
    final File uploadFile3 = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String file1 = "1_" + UPLOAD_FILE_NAME;
    final String file2 = "2_" + UPLOAD_FILE_NAME;
    final String file3 = "3_" + UPLOAD_FILE_NAME;
    final String nonExistingFile = "4_" + randomUUID();

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, file1, uploadFile1));
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, file2, uploadFile2));
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, file3, uploadFile3));

    final DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(BUCKET_NAME);

    final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
    keys.add(new DeleteObjectsRequest.KeyVersion(file1));
    keys.add(new DeleteObjectsRequest.KeyVersion(file2));
    keys.add(new DeleteObjectsRequest.KeyVersion(file3));
    keys.add(new DeleteObjectsRequest.KeyVersion(nonExistingFile));

    multiObjectDeleteRequest.setKeys(keys);

    final DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
    assertThat("Response should contain 3 entries",
        delObjRes.getDeletedObjects().size(), is(3));
    assertThat("Only existing files were reported as deleted",
        delObjRes.getDeletedObjects().stream().map(DeletedObject::getKey).collect(toList()),
        contains(file1, file2, file3));

    final AmazonS3Exception e = Assertions.assertThrows(AmazonS3Exception.class, () -> {
      s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
    });

    assertThat(e.getMessage(), containsString("Status Code: 404"));
  }

  /**
   * Tests that a bucket can be deleted.
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
   *
   * <p>For more detailed tests of the List Objects API see {@link ListObjectIT}.
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
   * Tests if an object can be uploaded asynchronously.
   *
   * @throws Exception not expected.
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
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  public void shouldPassUserMetadataWithMultipartUploads() {
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

    assertThat("User metadata should be identical!", metadataExisting.getUserMetadata(),
        is(equalTo(objectMetadata.getUserMetadata())));
  }

  @Test
  public void shouldInitiateMultipartAndRetrieveParts() throws IOException {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    final String hash = DigestUtils.md5Hex(new FileInputStream(uploadFile));
    final String expectedEtag = String.format("%s-%s", hash, 1);
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();
    final String key = initiateMultipartUploadResult.getKey();

    final UploadPartResult uploadPartResult = s3Client.uploadPart(new UploadPartRequest()
        .withBucketName(initiateMultipartUploadResult.getBucketName())
        .withKey(initiateMultipartUploadResult.getKey())
        .withUploadId(uploadId)
        .withFile(uploadFile)
        .withFileOffset(0)
        .withPartNumber(1)
        .withPartSize(uploadFile.length())
        .withLastPart(true));

    ListPartsRequest listPartsRequest = new ListPartsRequest(
        BUCKET_NAME,
        key,
        uploadId
    );
    final PartListing partListing = s3Client.listParts(listPartsRequest);

    assertThat("Part listing should be 1", partListing.getParts().size(), is(1));
    PartSummary partSummary = partListing.getParts().get(0);

    assertThat("Etag should match", partSummary.getETag(), is(expectedEtag));
    assertThat("Part number should match", partSummary.getPartNumber(), is(1));
    assertThat("LastModified should be valid date", partSummary.getLastModified(), any(Date.class));

  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  public void shouldListMultipartUploads() {
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
   */
  @Test
  public void shouldAbortMultipartUpload() {
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

    final File downloadFile = File.createTempFile(randomUUID().toString(), null);
    final Download download = transferManager.download(
        new GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(1, 2), downloadFile);
    download.waitForCompletion();
    assertThat("Invalid file length", downloadFile.length(), is(2L));
    assertThat(download.getObjectMetadata().getInstanceLength(), is(uploadFile.length()));
    assertThat(download.getObjectMetadata().getContentLength(), is(2L));

    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(0, 1000),
            downloadFile)
        .waitForCompletion();
    assertThat("Invalid file length", downloadFile.length(), is(uploadFile.length()));
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  public void multipartCopy() throws InterruptedException {
    final int contentLen = 3 * _1MB;

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(contentLen);

    final String assumedSourceKey = randomUUID().toString();

    final Bucket sourceBucket = s3Client.createBucket(randomUUID().toString());
    final Bucket targetBucket = s3Client.createBucket(randomUUID().toString());

    final TransferManager transferManager = createTransferManager(_2MB, _1MB, _2MB, _1MB);

    final EtagInputStream sourceInputStream = new EtagInputStream(
        randomInputStream(contentLen), _1MB);
    final Upload upload = transferManager
        .upload(sourceBucket.getName(), assumedSourceKey, sourceInputStream, objectMetadata);

    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey(), is(assumedSourceKey));

    final String assumedDestinationKey = randomUUID().toString();
    final Copy copy =
        transferManager.copy(sourceBucket.getName(), assumedSourceKey, targetBucket.getName(),
            assumedDestinationKey);
    final CopyResult copyResult = copy.waitForCopyResult();
    assertThat(copyResult.getDestinationKey(), is(assumedDestinationKey));

    assertThat("Hashes for source and target S3Object do not match.",
        uploadResult.getETag(),
        is(sourceInputStream.getEtag()));
  }

  /**
   * Creates a bucket, stores a file, adds tags, retrieves tags and checks them for consistency.
   */
  @Test
  public void shouldAddAndRetrieveTags() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,
        s3Object.getKey());
    GetObjectTaggingResult getObjectTaggingResult = s3Client
        .getObjectTagging(getObjectTaggingRequest);

    // There shouldn't be any tags here
    assertThat("There shouldn't be any tags now", getObjectTaggingResult.getTagSet().size(), is(0));

    final List<Tag> tagList = new ArrayList<>();
    tagList.add(new Tag("foo", "bar"));

    final SetObjectTaggingRequest setObjectTaggingRequest =
        new SetObjectTaggingRequest(BUCKET_NAME, s3Object.getKey(), new ObjectTagging(tagList));
    s3Client.setObjectTagging(setObjectTaggingRequest);

    getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME, s3Object.getKey());
    getObjectTaggingResult = s3Client.getObjectTagging(getObjectTaggingRequest);

    // There should be 'foo:bar' here
    assertThat("Couldn't find that the tag that was placed",
        getObjectTaggingResult.getTagSet().size(), is(1));
    assertThat("The vaule of the tag placed did not match",
        getObjectTaggingResult.getTagSet().get(0).getValue(), is("bar"));
  }

  /**
   * Creates a bucket, stores a file with tags, retrieves tags and checks them for consistency.
   */
  @Test
  public void canAddTagsOnPutObject() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final List<Tag> tagList = new ArrayList<>();
    tagList.add(new Tag("foo", "bar"));

    final PutObjectRequest putObjectRequest =
            new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile)
                    .withTagging(new ObjectTagging(tagList));

    s3Client.putObject(putObjectRequest);

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,
            s3Object.getKey());

    GetObjectTaggingResult getObjectTaggingResult = s3Client
            .getObjectTagging(getObjectTaggingRequest);

    // There should be 'foo:bar' here
    assertThat("Couldn't find that the tag that was placed",
            getObjectTaggingResult.getTagSet().size(), is(1));
    assertThat("The vaule of the tag placed did not match",
            getObjectTaggingResult.getTagSet().get(0).getValue(), is("bar"));
  }

  /**
   * Creates a bucket, stores a file, get files with eTag requrements.
   */
  @Test
  public void shouldCreateAndRespectEtag() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    final PutObjectResult returnObj = s3Client.putObject(
        new PutObjectRequest(BUCKET_NAME,
            uploadFile.getName(),
            uploadFile));

    // wit eTag
    GetObjectRequest requestWithEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    requestWithEtag.setMatchingETagConstraints(singletonList(returnObj.getETag()));

    GetObjectRequest requestWithHoutEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    // Create a new eTag that will not match
    final int notEtag = returnObj.getETag().hashCode();

    requestWithHoutEtag.setNonmatchingETagConstraints(singletonList(String.valueOf(notEtag)));

    final S3Object s3ObjectWithEtag = s3Client.getObject(requestWithEtag);
    final S3Object s3ObjectWithHoutEtag = s3Client.getObject(requestWithHoutEtag);

    final String s3ObjectWithEtagDownloadedHash = HashUtil
        .getDigest(s3ObjectWithEtag.getObjectContent());
    final String s3ObjectWithHoutEtagDownloadedHash = HashUtil
        .getDigest(s3ObjectWithHoutEtag.getObjectContent());

    final InputStream uploadFileIs = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIs);

    assertThat("The uploaded file and the recived file should be the same, "
            + "when requeting file which matchin eTag given same eTag",
        uploadHash, is(equalTo(s3ObjectWithEtagDownloadedHash)));
    assertThat("The uploaded file and the recived file should be the same, "
            + "when requeting file with  non-matchin eTag but given different eTag",
        uploadHash, is(equalTo(s3ObjectWithHoutEtagDownloadedHash)));

    // wit eTag
    requestWithEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    requestWithEtag.setMatchingETagConstraints(singletonList(String.valueOf(notEtag)));

    requestWithHoutEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    requestWithHoutEtag.setNonmatchingETagConstraints(singletonList(returnObj.getETag()));

    final S3Object s3ObjectWithEtagNull = s3Client.getObject(requestWithEtag);
    final S3Object s3ObjectWithHoutEtagNull = s3Client.getObject(requestWithHoutEtag);

    assertThat("Get Object with matching eTag should not return object if no eTag matches",
        s3ObjectWithEtagNull, is(equalTo(null)));
    assertThat("Get Object with non-matching eTag should not return object if eTag matches",
        s3ObjectWithHoutEtagNull, is(equalTo(null)));
  }
}
