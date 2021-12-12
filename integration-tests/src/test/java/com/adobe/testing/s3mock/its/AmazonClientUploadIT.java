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
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Test the application using the AmazonS3 client.
 */
class AmazonClientUploadIT extends S3TestBase {

  /**
   * Verify that buckets can be created and listed.
   */
  @Test
  void shouldCreateBucketAndListAllBuckets() {
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    // and account for a clock-skew in the Docker container of up to a minute.
    final Date creationDate = new Date((System.currentTimeMillis() / 1000) * 1000 - 60000);

    final Bucket bucket = s3Client.createBucket(BUCKET_NAME);
    assertThat(bucket.getName()).as(String.format("Bucket name should match '%s'!", BUCKET_NAME))
        .isEqualTo(BUCKET_NAME);

    final List<Bucket> buckets =
        s3Client.listBuckets().stream().filter(b -> BUCKET_NAME.equals(b.getName()))
            .collect(toList());

    assertThat(buckets).as("Expecting one bucket").hasSize(1);
    final Bucket createdBucket = buckets.get(0);

    assertThat(createdBucket.getCreationDate()).isAfterOrEqualTo(creationDate);
    final Owner bucketOwner = createdBucket.getOwner();
    assertThat(bucketOwner.getDisplayName()).isEqualTo("s3-mock-file-store");
    assertThat(bucketOwner.getId()).isEqualTo("123");
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  void defaultBucketsGotCreated() {
    final List<Bucket> buckets = s3Client.listBuckets();
    final Set<String> bucketNames = buckets.stream().map(Bucket::getName)
        .filter(INITIAL_BUCKET_NAMES::contains).collect(Collectors.toSet());

    assertThat(bucketNames).as("Not all default Buckets got created")
        .containsAll(INITIAL_BUCKET_NAMES);
  }

  /**
   * Verifies {@link AmazonS3#doesObjectExist}.
   */
  @Test
  void putObjectWhereKeyContainsPathFragments() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    final boolean objectExist = s3Client.doesObjectExist(BUCKET_NAME, UPLOAD_FILE_NAME);
    assertThat(objectExist).isTrue();
  }

  /**
   * Stores files in a previously created bucket. List files using ListObjectsV2Request
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndListV2Objects() throws Exception {
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
    assertThat(listResult.getKeyCount()).isEqualTo(3);
    for (final S3ObjectSummary objectSummary : listResult.getObjectSummaries()) {
      assertThat(objectSummary.getKey()).contains(uploadFile.getName());
      final S3Object s3Object = s3Client.getObject(BUCKET_NAME, objectSummary.getKey());
      verifyObjectContent(uploadFile, s3Object);
    }
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   *
   * @throws Exception if FileStreams can not be read
   */
  @ParameterizedTest(
      name = ParameterizedTest.INDEX_PLACEHOLDER + " uploadWithSigning={0}, uploadChunked={1}")
  @CsvSource(value = {"true, true", "true, false", "false, true", "false, false"})
  void shouldUploadAndDownloadObject(final boolean uploadWithSigning,
      final boolean uploadChunked)
      throws Exception {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);

    final AmazonS3 uploadClient = defaultTestAmazonS3ClientBuilder()
        .withPayloadSigningEnabled(uploadWithSigning)
        .withChunkedEncodingDisabled(uploadChunked)
        .build();

    uploadClient.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    verifyObjectContent(uploadFile, s3Object);
  }

  /**
   * Uses weird, but valid characters in the key used to store an object.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldTolerateWeirdCharactersInObjectKey() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_+.,~|\"':^"
        + "\u1234\uabcd\u0001"; // non-ascii and unprintable stuff
    final String key = weirdStuff + uploadFile.getName() + weirdStuff;

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, key);

    verifyObjectContent(uploadFile, s3Object);
  }

  private void verifyObjectContent(final File uploadFile, final S3Object s3Object)
      throws NoSuchAlgorithmException, IOException {
    final InputStream uploadFileIs = new FileInputStream(uploadFile);
    final String uploadHash = HashUtil.getDigest(uploadFileIs);
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    uploadFileIs.close();
    s3Object.close();

    assertThat(uploadHash).as("Up- and downloaded Files should have equal Hashes")
        .isEqualTo(downloadedHash);
  }

  /**
   * Uses weird, but valid characters in the key used to store an object. Verifies
   * that ListObject returns the correct object names.
   */
  @Test
  void shouldListWithCorrectObjectNames() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_ .,~|\"':^"
        + "\u1234\uabcd\u0001"; // non-ascii and unprintable stuff
    final String prefix = "shouldListWithCorrectObjectNames/";
    final String key = prefix + weirdStuff + uploadFile.getName() + weirdStuff;

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ObjectListing listing = s3Client.listObjects(BUCKET_NAME, prefix);
    final List<S3ObjectSummary> summaries = listing.getObjectSummaries();

    assertThat(summaries).as("Must have exactly one match").hasSize(1);
    assertThat(summaries.get(0).getKey()).as("Object name must match").isEqualTo(key);
  }

  /**
   * Same as {@link #shouldListWithCorrectObjectNames()} but for V2 API.
   */
  @Test
  void shouldListV2WithCorrectObjectNames() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String weirdStuff = "\\$%&_ .,~|\"':^"
        + "\u1234\uabcd\u0001"; // non-ascii and unprintable stuff
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

    assertThat(summaries).as("Must have exactly one match").hasSize(1);
    assertThat(summaries.get(0).getKey()).as("Object name must match").isEqualTo(key);
  }

  /**
   * Uses a key that cannot be represented in XML without encoding. Then lists
   * the objects without encoding, expecting a parse exception and thus verifying
   * that the encoding parameter is honored.
   *
   * <p>This isn't the greatest way to test this functionality, however, there
   * is currently no low-level testing infrastructure in place.
   */
  @Test
  void shouldHonorEncodingType() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String prefix = "shouldHonorEncodingType/";
    final String key = prefix + "\u0001"; // key invalid in XML

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ListObjectsRequest lor = new ListObjectsRequest(BUCKET_NAME, prefix, null, null, null);
    lor.setEncodingType(""); // don't use encoding

    //Starting in Spring Boot 2.6, Jackson is not able to encode the key properly if it's not
    // encoded by S3Mock. S3ObjectSummary will have empty key in this case.
    final ObjectListing listing = s3Client.listObjects(lor);
    final List<S3ObjectSummary> summaries = listing.getObjectSummaries();
    assertThat(summaries).as("Must have exactly one match").hasSize(1);
    assertThat(summaries.get(0).getKey()).as("Object name must match").isEqualTo("");
  }

  /**
   * The same as {@link #shouldHonorEncodingType()} but for V2 API.
   */
  @Test
  void shouldHonorEncodingTypeV2() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final String prefix = "shouldHonorEncodingType/";
    final String key = prefix + "\u0001"; // key invalid in XML

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, uploadFile));

    final ListObjectsV2Request lorv2 = new ListObjectsV2Request();
    lorv2.setBucketName(BUCKET_NAME);
    lorv2.setPrefix(prefix);
    lorv2.setEncodingType(""); // don't use encoding

    //Starting in Spring Boot 2.6, Jackson is not able to encode the key properly if it's not
    // encoded by S3Mock. S3ObjectSummary will have empty key in this case.
    final ListObjectsV2Result listing = s3Client.listObjectsV2(lorv2);
    final List<S3ObjectSummary> summaries = listing.getObjectSummaries();

    assertThat(summaries).as("Must have exactly one match").hasSize(1);
    assertThat(summaries.get(0).getKey()).as("Object name must match").isEqualTo("");
  }

  /**
   * Stores a file in a previously created bucket. Downloads the file again and compares checksums
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndDownloadStream() throws Exception {
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

    assertThat(s3Object.getObjectMetadata().getContentEncoding())
        .as("Uploaded File should have Encoding-Type set")
        .isEqualTo(contentEncoding);

    final String uploadHash = HashUtil.getDigest(new ByteArrayInputStream(resource));
    final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
    s3Object.close();

    assertThat(uploadHash).as("Up- and downloaded Files should have equal Hashes")
        .isEqualTo(downloadedHash);
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  void shouldUploadWithEncryption() {
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

    assertThat(objectMetadata.getContentLength()).isEqualTo(uploadFile.length());

    assertThat(objectMetadata.getUserMetadata()).as("User metadata should be identical!")
        .isEqualTo(metadata.getUserMetadata());

  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  void shouldNotUploadWithWrongEncryptionKey() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    assertThatThrownBy(() -> s3Client.putObject(putObjectRequest))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException");
  }

  /**
   * Tests if Object can be uploaded with wrong KMS Key.
   */
  @Test
  void shouldNotUploadStreamingWithWrongEncryptionKey() {
    final byte[] bytes = UPLOAD_FILE_NAME.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);
    final String objectKey = randomUUID().toString();
    s3Client.createBucket(BUCKET_NAME);
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, stream, metadata);
    putObjectRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(TEST_WRONG_KEYREF));

    assertThatThrownBy(() -> s3Client.putObject(putObjectRequest))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException");
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObject() throws Exception {
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

    assertThat(copiedHash).as("Sourcefile and copied File should have same Hashes")
        .isEqualTo(putObjectResult.getETag());
  }

  /**
   * Puts an Object; Copies that object to a new bucket with new user meta data; Downloads the
   * object from the new bucket;
   * compares checksums of original and copied object; compares copied object user meta data with
   * the new user meta data specified during copy request.
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObjectWithNewUserMetadata() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey + "/withNewUserMetadata";
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile));

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");
    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    copyObjectRequest.setNewObjectMetadata(objectMetadata);
    s3Client.copyObject(copyObjectRequest);

    final S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat(copiedHash).as("Source file and copied File should have same Hashes")
        .isEqualTo(putObjectResult.getETag());
    assertThat(copiedObject.getObjectMetadata().getUserMetadata()).as(
            "User metadata should be identical!")
        .isEqualTo(objectMetadata.getUserMetadata());
  }

  /**
   * Puts an Object with some user metadata; Copies that object to a new bucket.
   * Downloads the object from the new bucket;
   * compares checksums of original and copied object; compares copied object user meta data with
   * the source object user metadata;
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObjectWithSourceUserMetadata() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = UPLOAD_FILE_NAME;
    final String destinationBucketName = "destinationbucket";
    final String destinationKey = "copyOf/" + sourceKey + "/withSourceObjectUserMetadata";
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(destinationBucketName);
    final ObjectMetadata sourceObjectMetadata = new ObjectMetadata();
    sourceObjectMetadata.addUserMetadata("key", "value");
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile);
    putObjectRequest.setMetadata(sourceObjectMetadata);
    final PutObjectResult putObjectResult =
        s3Client.putObject(putObjectRequest);

    final CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey);
    s3Client.copyObject(copyObjectRequest);

    final S3Object copiedObject =
        s3Client.getObject(destinationBucketName, destinationKey);

    final String copiedHash = HashUtil.getDigest(copiedObject.getObjectContent());
    copiedObject.close();

    assertThat(copiedHash).as("Source file and copied File should have same Hashes")
        .isEqualTo(putObjectResult.getETag());
    assertThat(copiedObject.getObjectMetadata().getUserMetadata()).as(
            "User metadata should be identical!")
        .isEqualTo(sourceObjectMetadata.getUserMetadata());
  }

  /**
   * Copy an object to a key needing URL escaping.
   *
   * @see #shouldCopyObject()
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObjectToKeyNeedingEscaping() throws Exception {
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

    assertThat(copiedHash).as("Source file and copied File should have same Hashes")
        .isEqualTo(putObjectResult.getETag());
  }

  /**
   * Copy an object from a key needing URL escaping.
   *
   * @see #shouldCopyObject()
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObjectFromKeyNeedingEscaping() throws Exception {
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

    assertThat(copiedHash).as("Source file and copied File should have same Hashes")
        .isEqualTo(putObjectResult.getETag());
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   *
   * @throws Exception if an Exception occurs
   */
  @Test
  void shouldCopyObjectEncrypted() throws Exception {
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
    assertThat(copyObjectResult.getETag()).as("ETag should match").isEqualTo(uploadHash);
    assertThat(metadata.getContentLength()).as("Files should have the same length")
        .isEqualTo(uploadFile.length());
  }

  /**
   * Tests that an object wont be copied with wrong encryption Key.
   */
  @Test
  void shouldNotObjectCopyWithWrongEncryptionKey() {
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

    assertThatThrownBy(() -> s3Client.copyObject(copyObjectRequest))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException");
  }

  /**
   * Creates a bucket and checks if it exists using {@link AmazonS3Client#doesBucketExist(String)}.
   */
  @Test
  void bucketShouldExist() {
    s3Client.createBucket(BUCKET_NAME);

    final Boolean doesBucketExist = s3Client.doesBucketExistV2(BUCKET_NAME);

    assertThat(doesBucketExist).as(
            String.format("The previously created bucket, '%s', should exist!", BUCKET_NAME))
        .isTrue();
  }

  /**
   * Checks if {@link AmazonS3Client#doesBucketExistV2(String)} is false on a not existing Bucket.
   */
  @Test
  void bucketShouldNotExist() {
    final Boolean doesBucketExist = s3Client.doesBucketExistV2(BUCKET_NAME);

    assertThat(doesBucketExist).as(
            String.format("The bucket, '%s', should not exist!", BUCKET_NAME))
        .isFalse();
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  void shouldGetObjectMetadata() {
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

    assertThat(metadataExisting.getContentEncoding()).as("Content-Encoding should be identical!")
        .isEqualTo(putObjectResult.getMetadata().getContentEncoding());
    assertThat(metadataExisting.getETag()).as("The ETags should be identical!")
        .isEqualTo(putObjectResult.getETag());
    assertThat(metadataExisting.getUserMetadata()).as("User metadata should be identical!")
        .isEqualTo(objectMetadata.getUserMetadata());

    assertThatThrownBy(() -> s3Client.getObjectMetadata(BUCKET_NAME, nonExistingFileName))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 404");
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  void shouldDeleteObject() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    s3Client.deleteObject(BUCKET_NAME, UPLOAD_FILE_NAME);

    assertThatThrownBy(() -> s3Client.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 404");
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  void shouldBatchDeleteObjects() {
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
    assertThat(delObjRes.getDeletedObjects().size()).as("Response should contain 3 entries")
        .isEqualTo(3);
    assertThat(delObjRes.getDeletedObjects().stream().map(DeletedObject::getKey).collect(toList()))
        .as("Only existing files were reported as deleted").contains(file1, file2, file3);

    assertThatThrownBy(() -> s3Client.getObjectMetadata(BUCKET_NAME, UPLOAD_FILE_NAME))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 404");
  }

  /**
   * Tests that a bucket can be deleted.
   */
  @Test
  void shouldDeleteBucket() {
    s3Client.createBucket(BUCKET_NAME);
    s3Client.deleteBucket(BUCKET_NAME);

    final Boolean doesBucketExist = s3Client.doesBucketExist(BUCKET_NAME);
    assertThat(doesBucketExist).as("Deleted Bucket should not exist!").isFalse();
  }

  /**
   * Tests that a non-empty bucket cannot be deleted.
   */
  @Test
  void shouldNotDeleteNonEmptyBucket() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    assertThatThrownBy(() -> s3Client.deleteBucket(BUCKET_NAME))
        .isInstanceOf(AmazonS3Exception.class)
        .hasMessageContaining("Status Code: 409; Error Code: BucketNotEmpty");
  }

  /**
   * Tests if the list objects can be retrieved.
   *
   * <p>For more detailed tests of the List Objects API see {@link ListObjectIT}.
   */
  @Test
  void shouldGetObjectListing() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));

    final ObjectListing objectListingResult =
        s3Client.listObjects(BUCKET_NAME, UPLOAD_FILE_NAME);

    assertThat(objectListingResult.getObjectSummaries()).as("ObjectListing has no S3Objects.")
        .hasSizeGreaterThan(0);
    assertThat(objectListingResult.getObjectSummaries().get(0).getKey())
        .as("The Name of the first S3ObjectSummary item has not expected the key name.")
        .isEqualTo(UPLOAD_FILE_NAME);
  }

  /**
   * Tests if an object can be uploaded asynchronously.
   *
   * @throws Exception not expected.
   */
  @Test
  void shouldUploadInParallel() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final TransferManager transferManager = createDefaultTransferManager();
    final Upload upload =
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile));
    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey()).isEqualTo(UPLOAD_FILE_NAME);

    final S3Object getResult = s3Client.getObject(BUCKET_NAME, UPLOAD_FILE_NAME);
    assertThat(getResult.getKey()).isEqualTo(UPLOAD_FILE_NAME);
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  void checkRangeDownloads() throws Exception {
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
    assertThat(downloadFile.length()).as("Invalid file length").isEqualTo(2L);
    assertThat(download.getObjectMetadata().getInstanceLength()).isEqualTo(uploadFile.length());
    assertThat(download.getObjectMetadata().getContentLength()).isEqualTo(2L);

    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME).withRange(0, 1000),
            downloadFile)
        .waitForCompletion();
    assertThat(downloadFile.length()).as("Invalid file length").isEqualTo(uploadFile.length());
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  void multipartCopy() throws InterruptedException {
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

    assertThat(uploadResult.getKey()).isEqualTo(assumedSourceKey);

    final String assumedDestinationKey = randomUUID().toString();
    final Copy copy =
        transferManager.copy(sourceBucket.getName(), assumedSourceKey, targetBucket.getName(),
            assumedDestinationKey);
    final CopyResult copyResult = copy.waitForCopyResult();
    assertThat(copyResult.getDestinationKey()).isEqualTo(assumedDestinationKey);

    assertThat(uploadResult.getETag()).as("Hashes for source and target S3Object do not match.")
        .isEqualTo(sourceInputStream.getEtag());
  }

  /**
   * Creates a bucket, stores a file, adds tags, retrieves tags and checks them for consistency.
   */
  @Test
  void shouldAddAndRetrieveTags() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,
        s3Object.getKey());
    GetObjectTaggingResult getObjectTaggingResult = s3Client
        .getObjectTagging(getObjectTaggingRequest);

    // There shouldn't be any tags here
    assertThat(getObjectTaggingResult.getTagSet()).as("There shouldn't be any tags now").hasSize(0);

    final List<Tag> tagList = new ArrayList<>();
    tagList.add(new Tag("foo", "bar"));

    final SetObjectTaggingRequest setObjectTaggingRequest =
        new SetObjectTaggingRequest(BUCKET_NAME, s3Object.getKey(), new ObjectTagging(tagList));
    s3Client.setObjectTagging(setObjectTaggingRequest);

    getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME, s3Object.getKey());
    getObjectTaggingResult = s3Client.getObjectTagging(getObjectTaggingRequest);

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.getTagSet()).as("Couldn't find that the tag that was placed")
        .hasSize(1);
    assertThat(getObjectTaggingResult.getTagSet().get(0).getValue()).as(
        "The value of the tag placed did not match").isEqualTo("bar");
  }

  /**
   * Creates a bucket, stores a file with tags, retrieves tags and checks them for consistency.
   */
  @Test
  void canAddTagsOnPutObject() {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    final List<Tag> tagList = new ArrayList<>();
    tagList.add(new Tag("foo", "bar"));

    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile)
            .withTagging(new ObjectTagging(tagList));

    s3Client.putObject(putObjectRequest);

    final S3Object s3Object = s3Client.getObject(BUCKET_NAME, uploadFile.getName());

    final GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME,
        s3Object.getKey());

    final GetObjectTaggingResult getObjectTaggingResult = s3Client
        .getObjectTagging(getObjectTaggingRequest);

    // There should be 'foo:bar' here
    assertThat(getObjectTaggingResult.getTagSet()).as("Couldn't find that the tag that was placed")
        .hasSize(1);
    assertThat(getObjectTaggingResult.getTagSet().get(0).getValue()).as(
        "The value of the tag placed did not match").isEqualTo("bar");
  }

  /**
   * Creates a bucket, stores a file, get files with eTag requirements.
   */
  @Test
  void shouldCreateAndRespectEtag() throws Exception {
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

    assertThat(uploadHash).as("The uploaded file and the recived file should be the same, "
            + "when requesting file with matching eTag given same eTag")
        .isEqualTo(s3ObjectWithEtagDownloadedHash);
    assertThat(uploadHash).as("The uploaded file and the recived file should be the same, "
            + "when requesting file with  non-matching eTag but given different eTag")
        .isEqualTo(s3ObjectWithHoutEtagDownloadedHash);

    // wit eTag
    requestWithEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    requestWithEtag.setMatchingETagConstraints(singletonList(String.valueOf(notEtag)));

    requestWithHoutEtag = new GetObjectRequest(BUCKET_NAME, uploadFile.getName());
    requestWithHoutEtag.setNonmatchingETagConstraints(singletonList(returnObj.getETag()));

    final S3Object s3ObjectWithEtagNull = s3Client.getObject(requestWithEtag);
    final S3Object s3ObjectWithHoutEtagNull = s3Client.getObject(requestWithHoutEtag);

    assertThat(s3ObjectWithEtagNull).as(
            "Get Object with matching eTag should not return object if no eTag matches")
        .isNull();
    assertThat(s3ObjectWithHoutEtagNull).as(
            "Get Object with non-matching eTag should not return object if eTag matches")
        .isNull();
  }

  @Test
  void generatePresignedUrlWithResponseHeaderOverrides()
      throws IOException, NoSuchAlgorithmException, KeyManagementException {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);
    s3Client.putObject(
        new PutObjectRequest(BUCKET_NAME,
            uploadFile.getName(),
            uploadFile));

    final GeneratePresignedUrlRequest presignedUrlRequest =
        new GeneratePresignedUrlRequest(BUCKET_NAME, uploadFile.getName());

    final ResponseHeaderOverrides overrides = new ResponseHeaderOverrides();
    overrides.setCacheControl("cacheControl");
    overrides.setContentDisposition("contentDisposition");
    overrides.setContentEncoding("contentEncoding");
    overrides.setContentLanguage("contentLanguage");
    overrides.setContentType("contentType");
    overrides.setExpires("expires");
    presignedUrlRequest.withResponseHeaders(overrides);

    final URL resourceUrl = s3Client.generatePresignedUrl(presignedUrlRequest);

    final URLConnection urlConnection = openUrlConnection(resourceUrl);
    assertThat(urlConnection.getHeaderField(Headers.CACHE_CONTROL))
        .isEqualTo("cacheControl");
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_DISPOSITION))
        .isEqualTo("contentDisposition");
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_ENCODING))
        .isEqualTo("contentEncoding");
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_LANGUAGE))
        .isEqualTo("contentLanguage");
    assertThat(urlConnection.getHeaderField(Headers.CONTENT_TYPE))
        .isEqualTo("contentType");
    assertThat(urlConnection.getHeaderField(Headers.EXPIRES))
        .isEqualTo("expires");
    urlConnection.getInputStream().close();
  }

  private URLConnection openUrlConnection(final URL resourceUrl)
      throws NoSuchAlgorithmException, KeyManagementException, IOException {
    final TrustManager[] trustAllCerts = new TrustManager[] {
        new X509TrustManager() {
          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          @Override
          public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
          }

          @Override
          public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
          }
        }
    };
    final SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(
        (hostname, sslSession) -> hostname.equals("localhost"));
    final URLConnection urlConnection = resourceUrl.openConnection();
    urlConnection.connect();
    return urlConnection;
  }
}
