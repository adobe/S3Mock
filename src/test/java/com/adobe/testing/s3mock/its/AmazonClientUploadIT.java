/*
 *  Copyright 2017 Adobe.
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

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test the application using the AmazonS3 client.
 */
@SuppressWarnings("javadoc")
public class AmazonClientUploadIT {
  private static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket-a", "bucket-b");

  private static final String TEST_ENC_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref";

  private static final String BUCKET_NAME = "mydemotestbucket";

  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  private static final String TEST_WRONG_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:keyWRONGWRONGWRONG/2d70f7f6-b484-4309-91d5-7813b7dd46ce";
  private static final int _1MB = 1024 * 1024;
  private static final long _2MB = 2L * _1MB;
  private static final long _6BYTE = 6L;

  private static final int THREAD_COUNT = 50;

  private AmazonS3 s3Client;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Configures the S3-Client to be used in the Test. Sets the SSL context to accept untrusted SSL
   * connections.
   */
  @Before
  public void prepareS3Client() {
    final BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

    s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withClientConfiguration(ignoringInvalidSslCertificates(new ClientConfiguration()))
        .withEndpointConfiguration(
            new EndpointConfiguration("https://" + getHost() + ":" + getPort(), "us-east-1"))
        .enablePathStyleAccess()
        .build();
  }

  /**
   * Deletes all existing buckets
   */
  @After
  public void cleanupFilestore() {
    for (final Bucket bucket : s3Client.listBuckets()) {
      if (!INITIAL_BUCKET_NAMES.contains(bucket.getName())) {
        s3Client.deleteBucket(bucket.getName());
      }
    }
  }

  private String getHost() {
    return System.getProperty("it.s3mock.host", "localhost");
  }

  private int getPort() {
    return Integer.getInteger("it.s3mock.port_https", 9191);
  }

  /**
   * Verify that buckets can be created and listed
   */
  @Test
  public void shouldCreateBucketAndListAllBuckets() throws Exception {
    // the returned creation date might strip off the millisecond-part, resulting in rounding down
    final Date creationDate = new Date((System.currentTimeMillis() / 1000) * 1000);

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
    final String objectKey = uploadFile.getName();
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
    final String objectKey = uploadFile.getName();
    s3Client.createBucket(BUCKET_NAME);
    final PutObjectRequest putObjectRequest =
        new PutObjectRequest(BUCKET_NAME, objectKey, uploadFile);
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
    final String sourceKey = uploadFile.getName();
    final String destinationBucketName = "destinationBucket";
    final String destinationKey = "copyOf" + sourceKey;

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
    final String sourceKey = uploadFile.getName();
    final String destinationBucketName = "destinationBucket";
    final String destinationKey = "copyOf" + sourceKey;

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
  public void shouldNotObjectCopyWithWrongEncryptionKey() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final String sourceKey = uploadFile.getName();
    final String destinationBucketName = "destinationBucket";
    final String destinationKey = "copyOf" + sourceKey;

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

    final PutObjectResult putObjectResult =
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));
    final ObjectMetadata metadataExisting =
        s3Client.getObjectMetadata(BUCKET_NAME, uploadFile.getName());

    assertThat("The ETags should be identically!", metadataExisting.getETag(),
        is(putObjectResult.getETag()));

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

    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));
    s3Client.deleteObject(BUCKET_NAME, uploadFile.getName());

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 406"));
    s3Client.getObject(BUCKET_NAME, uploadFile.getName());
  }

  /**
   * Tests if an object can be deleted
   */
  @Test
  public void shouldBatchDeleteObjects() {
    final File uploadFile1 = new File(UPLOAD_FILE_NAME);
    final File uploadFile2 = new File(UPLOAD_FILE_NAME);
    final File uploadFile3 = new File(UPLOAD_FILE_NAME);
    final File uploadFile4 = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(BUCKET_NAME);

    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "1_" + uploadFile1.getName(), uploadFile1));
    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "2_" + uploadFile2.getName(), uploadFile2));
    s3Client
        .putObject(new PutObjectRequest(BUCKET_NAME, "3_" + uploadFile3.getName(), uploadFile3));

    final DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(BUCKET_NAME);

    final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
    keys.add(new DeleteObjectsRequest.KeyVersion("1_" + uploadFile1.getName()));
    keys.add(new DeleteObjectsRequest.KeyVersion("2_" + uploadFile2.getName()));
    keys.add(new DeleteObjectsRequest.KeyVersion("3_" + uploadFile3.getName()));
    keys.add(new DeleteObjectsRequest.KeyVersion("4_" + uploadFile4.getName()));

    multiObjectDeleteRequest.setKeys(keys);

    final DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
    assertThat("Response should contain 4 entries", delObjRes.getDeletedObjects().size(), is(4));

    thrown.expect(AmazonS3Exception.class);
    thrown.expectMessage(containsString("Status Code: 406"));

    s3Client.getObject(BUCKET_NAME, uploadFile3.getName());
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
    s3Client.putObject(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));

    final ObjectListing objectListingResult =
        s3Client.listObjects(BUCKET_NAME, uploadFile.getName());

    assertThat("ObjectListinig has no S3Objects.",
        objectListingResult.getObjectSummaries().size(),
        is(greaterThan(0)));
    assertThat("The Name of the first S3ObjectSummary item has not expected the key name.",
        objectListingResult.getObjectSummaries().get(0).getKey(),
        is(uploadFile.getName()));
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
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));
    final UploadResult uploadResult = upload.waitForUploadResult();

    assertThat(uploadResult.getKey(), equalTo(uploadFile.getName()));

    final S3Object getResult = s3Client.getObject(BUCKET_NAME, uploadFile.getName());
    assertThat(getResult.getKey(), equalTo(uploadFile.getName()));
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
        transferManager.upload(new PutObjectRequest(BUCKET_NAME, uploadFile.getName(), uploadFile));
    upload.waitForUploadResult();

    final File downloadFile = File.createTempFile(UUID.randomUUID().toString(), null);
    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, uploadFile.getName()).withRange(1, 2),
            downloadFile)
        .waitForCompletion();
    assertThat("Invalid file length", downloadFile.length(), is(2L));

    transferManager
        .download(new GetObjectRequest(BUCKET_NAME, uploadFile.getName()).withRange(0, 1000),
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

  private TransferManager createDefaultTransferManager() {
    return createTransferManager(_6BYTE, _6BYTE, _6BYTE, _6BYTE);
  }

  private TransferManager createTransferManager(final long multipartUploadThreshold,
      final long multipartUploadPartSize,
      final long multipartCopyThreshold,
      final long multipartCopyPartSize) {
    final ThreadFactory threadFactory = new ThreadFactory() {
      private int threadCount = 1;

      @Override
      public Thread newThread(final Runnable r) {
        final Thread thread = new Thread(r);
        thread.setName("s3-transfer-" + this.threadCount++);

        return thread;
      }
    };

    return TransferManagerBuilder.standard()
        .withS3Client(s3Client)
        .withExecutorFactory(() -> Executors.newFixedThreadPool(THREAD_COUNT, threadFactory))
        .withMultipartUploadThreshold(multipartUploadThreshold)
        .withMinimumUploadPartSize(multipartUploadPartSize)
        .withMultipartCopyPartSize(multipartCopyPartSize)
        .withMultipartCopyThreshold(multipartCopyThreshold)
        .build();
  }

  private InputStream randomInputStream(final int size) {
    final byte[] content = new byte[size];
    new Random().nextBytes(content);

    return new ByteArrayInputStream(content);
  }

  private ClientConfiguration ignoringInvalidSslCertificates(
      final ClientConfiguration clientConfiguration) {

    clientConfiguration.getApacheHttpClientConfig()
        .withSslSocketFactory(new SSLConnectionSocketFactory(
            createBlindlyTrustingSSLContext(),
            NoopHostnameVerifier.INSTANCE));

    return clientConfiguration;
  }

  private SSLContext createBlindlyTrustingSSLContext() {
    try {
      final SSLContext sc = SSLContext.getInstance("TLS");

      sc.init(null, new TrustManager[] {new X509ExtendedTrustManager() {
        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2)
            throws CertificateException {
          // no-op
        }
      }
      }, new java.security.SecureRandom());

      return sc;
    } catch (final NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }
}
