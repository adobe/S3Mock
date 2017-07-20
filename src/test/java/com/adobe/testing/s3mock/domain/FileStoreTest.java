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

package com.adobe.testing.s3mock.domain;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.util.Files.contentOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

import com.adobe.testing.s3mock.domain.Bucket;
import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.S3Object;
import com.adobe.testing.s3mock.util.HashUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.entity.ContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.http.MediaType;

/**
 *
 *
 */
@SuppressWarnings("javadoc")
public class FileStoreTest {
  private static final String SIGNED_CONTENT =
      "24;chunk-signature=11707b33deb094881a16c70e9cbd5d79053a0bb235c25674e3cf0fed601683b5\r\n"
          + "## sample test file ##\n"
          + "\n"
          + "demo=content\n"
          + "0;chunk-signature=2206490f19c068b46367173d1e155b597fd367037fa3f924290b41c1e83c1c08";

  private static final String TEST_BUCKET_NAME = "testbucket";

  private static final String TEST_FILE_PATH = "src/test/resources/sampleFile.txt";

  private static final String TEST_ENC_TYPE = "aws:kms";

  private static final String TEST_ENC_KEY = "aws:kms" + UUID.randomUUID();

  private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

  private FileStore fileStore = null;

  private File rootFolder;

  @Rule
  public ExpectedException expectedExceptions = ExpectedException.none();

  /**
   * Instantiates the FileStore
   *
   * @throws Exception if an IOException occurrs
   */
  @Before
  public void prepare() throws Exception {
    rootFolder = new File("target", "s3mockFileStore" + new Date().getTime());
    this.fileStore = new FileStore(rootFolder);
  }

  /**
   * Creates a bucket and checks that it exists
   *
   * @throws Exception if the Bucket could not be created on the file system
   */
  @Test
  public void shouldCreateBucket() throws Exception {
    final Bucket bucket = fileStore.createBucket(TEST_BUCKET_NAME);
    assertThat("Bucket should have been created.", bucket.getName(), endsWith(TEST_BUCKET_NAME));
  }

  /**
   * Checks if Bucket exists
   *
   * @throws Exception if the Bucket could not be created on the file system
   */
  @Test
  public void bucketShouldExist() throws Exception {
    fileStore.createBucket(TEST_BUCKET_NAME);

    final Boolean doesBucketExist = fileStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(
        String.format("The previously created bucket, '%s', should exist!", TEST_BUCKET_NAME),
        doesBucketExist,
        is(true));
  }

  /**
   * Checks if bucket doesn't exist.
   *
   */
  @Test
  public void bucketShouldNotExist() {
    final Boolean doesBucketExist = fileStore.doesBucketExist(TEST_BUCKET_NAME);

    assertThat(String.format("The bucket, '%s', should not exist!", TEST_BUCKET_NAME),
        doesBucketExist, is(false));
  }

  /**
   * Checks if created buckets are listed
   *
   * @throws Exception if the Bucket could not be created on the file system
   */
  @Test
  public void shouldHoldAllBuckets() throws Exception {
    final String bucketName1 = "myNüwNämeÄins";
    final String bucketName2 = "myNüwNämeZwöei";
    final String bucketName3 = "myNüwNämeDrü";

    fileStore.createBucket(bucketName1);
    fileStore.createBucket(bucketName2);
    fileStore.createBucket(bucketName3);

    final List<Bucket> buckets = fileStore.listBuckets();

    assertThat("FileStore should hold three Buckets", buckets.size() == 3);
  }

  /**
   * Creates a bucket an checks that it can be retrieved by it's name
   *
   * @throws Exception if the Bucket could not be created on the file system
   */
  @Test
  public void shouldGetBucketByName() throws Exception {
    fileStore.createBucket(TEST_BUCKET_NAME);
    final Bucket bucket = fileStore.getBucket(TEST_BUCKET_NAME);

    assertNotNull("Bucket should not be null", bucket);
    assertThat("Bucket name should end with " + TEST_BUCKET_NAME, bucket.getName(),
        is(TEST_BUCKET_NAME));
  }

  /**
   * Checks that an object can be stored in a bucket
   *
   * @throws Exception If an Exception occurred
   */
  @Test
  public void shouldStoreFileInBucket() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);
    final String name = sourceFile.getName();
    final String contentType = ContentType.TEXT_PLAIN.toString();
    final String md5 = HashUtil.getDigest(new FileInputStream(sourceFile));
    final String size = Long.toString(sourceFile.length());

    final S3Object returnedObject =
        fileStore.putS3Object(TEST_BUCKET_NAME, name, contentType, new FileInputStream(sourceFile),
            false);

    assertThat("Name should be '" + name + "'", returnedObject.getName(), is(name));
    assertThat("ContentType should be '" + contentType + "'", returnedObject.getContentType(),
        is(contentType));
    assertThat("M5 should be '" + md5 + "'", returnedObject.getMd5(), is(md5));
    assertThat("Size should be '" + size + "'", returnedObject.getSize(), is(size));
    assertThat("File should not be encrypted!", !returnedObject.isEncrypted());

    assertThat("Files should be equal", contentOf(sourceFile, UTF_8),
        is(contentOf(returnedObject.getDataFile(), UTF_8)));
  }

  /**
   * Checks that an object can be stored in a bucket
   *
   * @throws Exception If an Exception occurred
   */
  @Test
  public void shouldStoreObjectEncrypted() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);

    final String name = sourceFile.getName();
    final String contentType = ContentType.TEXT_PLAIN.toString();
    final String md5 = HashUtil.getDigest(TEST_ENC_KEY, new FileInputStream(sourceFile));

    final S3Object returnedObject =
        fileStore.putS3ObjectWithKMSEncryption(TEST_BUCKET_NAME,
            name,
            contentType,
            new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
            true,
            TEST_ENC_TYPE,
            TEST_ENC_KEY);

    assertThat("Filelength matches", returnedObject.getSize(), is("36"));
    assertThat("File should be encrypted", returnedObject.isEncrypted());
    assertThat("Encryption Type matches", returnedObject.getKmsEncryption(), is(TEST_ENC_TYPE));
    assertThat("Encryption Key matches", returnedObject.getKmsKeyId(), is(TEST_ENC_KEY));
    assertThat("MD5 should not match", returnedObject.getMd5(), is(md5));
  }

  /**
   * Checks that an object can be stored in a bucket
   *
   * @throws Exception If an Exception occurred
   */
  @Test
  public void shouldGetEncryptedObject() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);

    final String name = sourceFile.getName();
    final String contentType = ContentType.TEXT_PLAIN.toString();
    final String md5 = HashUtil.getDigest(TEST_ENC_KEY, new FileInputStream(sourceFile));

    fileStore.putS3ObjectWithKMSEncryption(TEST_BUCKET_NAME,
        name,
        contentType,
        new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
        true,
        TEST_ENC_TYPE,
        TEST_ENC_KEY);

    final S3Object returnedObject = fileStore.getS3Object(TEST_BUCKET_NAME, name);

    assertThat("Filelength matches", returnedObject.getSize(), is("36"));
    assertThat("File should be encrypted", returnedObject.isEncrypted());
    assertThat("Encryption Type matches", returnedObject.getKmsEncryption(), is(TEST_ENC_TYPE));
    assertThat("Encryption Key matches", returnedObject.getKmsKeyId(), is(TEST_ENC_KEY));
    assertThat("MD5 should not match", returnedObject.getMd5(), is(md5));
  }

  /**
   * Checks that a previously created object can be retrieved from a bucket
   *
   * @throws Exception if an Exception occurred
   */
  @Test
  public void shouldGetFile() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);

    final String name = sourceFile.getName();
    final String contentType = ContentType.TEXT_PLAIN.toString();
    final String md5 = HashUtil.getDigest(new FileInputStream(sourceFile));
    final String size = Long.toString(sourceFile.length());

    fileStore
        .putS3Object(TEST_BUCKET_NAME, name, contentType, new FileInputStream(sourceFile), false);

    final S3Object returnedObject = fileStore.getS3Object(TEST_BUCKET_NAME, name);

    assertThat("Name should be '" + name + "'", returnedObject.getName(), is(name));
    assertThat("ContentType should be '" + contentType + "'", returnedObject.getContentType(),
        is(contentType));
    assertThat("M5 should be '" + md5 + "'", returnedObject.getMd5(), is(md5));
    assertThat("Size should be '" + size + "'", returnedObject.getSize(), is(size));
    assertThat("File should not be encrypted!", !returnedObject.isEncrypted());

    assertThat("Files should be equal!", contentOf(sourceFile, UTF_8),
        is(contentOf(returnedObject.getDataFile(), UTF_8)));
  }

  /**
   * Tests if an object can be copied from one to another bucket
   *
   * @throws Exception if files can't be read
   */
  @Test
  public void shouldCopyObject() throws Exception {
    final String destinationObjectName = "destinationObject";
    final String destinationBucketName = "destinationBucket";

    final File sourceFile = new File(TEST_FILE_PATH);

    final String sourceBucketName = "sourceBucket";
    final String contentType = "text/plain";
    final String sourceObjectName = sourceFile.getName();

    fileStore.putS3Object(sourceBucketName, sourceObjectName, contentType,
        new FileInputStream(sourceFile), false);

    fileStore.copyS3Object(sourceBucketName, sourceObjectName, destinationBucketName,
        destinationObjectName);
    final S3Object copiedObject =
        fileStore.getS3Object(destinationBucketName, destinationObjectName);

    assertThat("File should not be encrypted!", !copiedObject.isEncrypted());
    assertThat("Files should be equal!", contentOf(sourceFile, UTF_8),
        is(contentOf(copiedObject.getDataFile(), UTF_8)));
  }

  /**
   * Tests if an object can be copied from one to another bucket
   *
   * @throws Exception if files can't be read
   */
  @Test
  public void shouldCopyObjectEncrypted() throws Exception {
    final String destinationObjectName = "destinationObject";
    final String destinationBucketName = "destinationBucket";

    final File sourceFile = new File(TEST_FILE_PATH);

    final String sourceBucketName = "sourceBucket";
    final String contentType = "text/plain";
    final String sourceObjectName = sourceFile.getName();
    final String md5 = HashUtil.getDigest(TEST_ENC_KEY, new FileInputStream(sourceFile));

    fileStore.putS3Object(sourceBucketName, sourceObjectName, contentType,
        new FileInputStream(sourceFile), false);

    fileStore.copyS3ObjectEncrypted(sourceBucketName,
        sourceObjectName,
        destinationBucketName,
        destinationObjectName,
        TEST_ENC_TYPE,
        TEST_ENC_KEY);

    final S3Object copiedObject =
        fileStore.getS3Object(destinationBucketName, destinationObjectName);

    assertThat("File should be encrypted!", copiedObject.isEncrypted());
    assertThat(
        "Files should have the same length", copiedObject.getSize(),
        is(String.valueOf(sourceFile.length())));
    assertThat("MD5 should match", copiedObject.getMd5(), is(md5));
  }

  /**
   * Tests if an object can be deleted.
   *
   * @throws Exception if an FileNotFoundException or IOException is thrown
   */
  @Test
  public void shouldDeleteObject() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);

    final String objectName = sourceFile.getName();

    fileStore
        .putS3Object(TEST_BUCKET_NAME, objectName, "text/plain", new FileInputStream(sourceFile),
            false);
    final boolean objectDeleted = fileStore.deleteObject(TEST_BUCKET_NAME, objectName);
    final S3Object s3Object = fileStore.getS3Object(TEST_BUCKET_NAME, objectName);

    assertThat("Deletion should succeed!", objectDeleted, is(true));
    assertThat("Object should be null!", s3Object, is(nullValue()));
  }

  /**
   * Checks if a bucket can be deleted
   *
   * @throws Exception if an Exception occurred
   */
  @Test
  public void shoudDeleteBucket() throws Exception {
    final File sourceFile = new File(TEST_FILE_PATH);
    final String objectName = sourceFile.getName();

    fileStore.createBucket(TEST_BUCKET_NAME);
    fileStore
        .putS3Object(TEST_BUCKET_NAME, objectName, "text/plain", new FileInputStream(sourceFile),
            false);

    final boolean bucketDeleted = fileStore.deleteBucket(TEST_BUCKET_NAME);

    final Bucket bucket = fileStore.getBucket(TEST_BUCKET_NAME);

    assertThat("Bucket should be delted!", bucketDeleted, is(true));
    assertThat("Bucket should be null!", bucket, is(nullValue()));
  }

  @Test
  public void shouldCreateMultipartUploadFolder() {
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, "aFile", DEFAULT_CONTENT_TYPE, "12345");

    final File destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, "aFile", "12345").toFile();

    assertThat("Destination folder does not exist", destinationFolder.exists(), is(true));
    assertThat("Destination folder is not a directory", destinationFolder.isDirectory(), is(true));
  }

  @Test
  public void shouldCreateMultipartUploadFolderIfBucketExists() throws IOException {
    fileStore.createBucket(TEST_BUCKET_NAME);
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, "aFile", DEFAULT_CONTENT_TYPE, "12345");

    final File destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, "aFile", "12345").toFile();

    assertThat("Destination folder does not exist", destinationFolder.exists(), is(true));
    assertThat("Destination folder is not a directory", destinationFolder.isDirectory(), is(true));
  }

  @Test
  public void shouldStorePart() throws Exception {

    final String fileName = "PartFile";
    final String uploadId = "12345";
    final String partNumber = "1";
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, fileName, DEFAULT_CONTENT_TYPE, uploadId);

    fileStore.putPart(
        TEST_BUCKET_NAME, fileName, uploadId, partNumber,
        new ByteArrayInputStream("Test".getBytes()), false);

    assertThat("Part does not exist!",
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, uploadId,
            partNumber + ".part")
            .toFile()
            .exists(),
        is(true));
  }

  @Test
  public void shouldFinishUpload() throws Exception {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, fileName, DEFAULT_CONTENT_TYPE, uploadId);
    fileStore
        .putPart(TEST_BUCKET_NAME, fileName, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false);
    fileStore
        .putPart(TEST_BUCKET_NAME, fileName, uploadId, "2",
            new ByteArrayInputStream("Part2".getBytes()), false);

    final String etag = fileStore.completeMultipartUpload(TEST_BUCKET_NAME, fileName, uploadId);

    assertThat("File does not exist!",
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, "fileData").toFile()
            .exists(),
        is(true));
    assertThat("Metadata does not exist!",
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, "metadata").toFile()
            .exists(),
        is(true));
    assertThat("Special etag doesn't match.",
        new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest("Part1Part2".getBytes())))
            + "-1",
        equalTo(etag));
  }

  @Test
  public void hasValidMetadata() throws Exception {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, fileName, DEFAULT_CONTENT_TYPE, uploadId);
    fileStore
        .putPart(TEST_BUCKET_NAME, fileName, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false);
    fileStore
        .putPart(TEST_BUCKET_NAME, fileName, uploadId, "2",
            new ByteArrayInputStream("Part2".getBytes()), false);

    fileStore.completeMultipartUpload(TEST_BUCKET_NAME, fileName, uploadId);

    final S3Object s3Object = fileStore.getS3Object(TEST_BUCKET_NAME, "PartFile");
    assertThat("Size doesn't match.", s3Object.getSize(), is("10"));
    assertThat(s3Object.getContentType(), is(MediaType.APPLICATION_OCTET_STREAM_VALUE));
  }

  @Test
  public void deletesTemporaryMultipartUploadFolder() throws Exception {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, fileName, DEFAULT_CONTENT_TYPE, uploadId);
    fileStore
        .putPart(TEST_BUCKET_NAME, fileName, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false);

    fileStore.completeMultipartUpload(TEST_BUCKET_NAME, fileName, uploadId);

    assertThat("Folder should not exist anymore!",
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, uploadId).toFile()
            .exists(),
        is(false));
  }

  @Test
  public void copyPart() throws Exception {

    final String sourceFile = UUID.randomUUID().toString();
    final String uploadId = UUID.randomUUID().toString();

    final String targetFile = UUID.randomUUID().toString();
    final String partNumber = "1";

    final byte[] contentBytes = UUID.randomUUID().toString().getBytes();
    fileStore.putS3Object(TEST_BUCKET_NAME, sourceFile, DEFAULT_CONTENT_TYPE,
        new ByteArrayInputStream(contentBytes), false);

    fileStore.prepareMultipartUpload(TEST_BUCKET_NAME, targetFile, DEFAULT_CONTENT_TYPE, uploadId);

    fileStore.copyPart(
        TEST_BUCKET_NAME, sourceFile,0, contentBytes.length, false, partNumber,
        TEST_BUCKET_NAME, targetFile, uploadId);

    assertThat("Part does not exist!",
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, targetFile, uploadId,
            partNumber + ".part")
            .toFile()
            .exists(),
        is(true));
  }

  @Test
  public void missingUploadPreparation() throws Exception {
    expectedExceptions.expect(IllegalStateException.class);
    expectedExceptions.expectMessage("Missed preparing Multipart Request");

    fileStore.copyPart(
        TEST_BUCKET_NAME, UUID.randomUUID().toString(),0, 0, false, "1",
        TEST_BUCKET_NAME, UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  /**
   * Deletes all existing buckets
   *
   * @throws Exception if bucket could not be deleted
   */
  @After
  public void cleanupFilestore() throws Exception {
    for (final Bucket bucket : fileStore.listBuckets()) {
      fileStore.deleteBucket(bucket.getName());
    }
  }

}
