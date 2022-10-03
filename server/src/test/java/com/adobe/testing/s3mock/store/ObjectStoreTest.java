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

package com.adobe.testing.s3mock.store;

import static com.adobe.testing.s3mock.dto.Grant.Permission.FULL_CONTROL;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Files.contentOf;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = {KmsKeyStore.class, BucketStore.class, MultipartStore.class})
@SpringBootTest(classes = {StoreConfiguration.class})
@Execution(SAME_THREAD)
class ObjectStoreTest extends StoreTestBase {
  private static final String SIGNED_CONTENT =
      "24;chunk-signature=11707b33deb094881a16c70e9cbd5d79053a0bb235c25674e3cf0fed601683b5\r\n"
          + "## sample test file ##\n"
          + "\n"
          + "demo=content\n"
          + "0;chunk-signature=2206490f19c068b46367173d1e155b597fd367037fa3f924290b41c1e83c1c08";
  private static final String UNSIGNED_CONTENT =
      "## sample test file ##\n"
          + "\n"
          + "demo=content";
  private static final List<UUID> idCache = Collections.synchronizedList(new ArrayList<>());

  @Autowired
  private ObjectStore objectStore;

  @BeforeEach
  void beforeEach() {
    assertThat(idCache).isEmpty();
  }

  @Test
  void testStoreObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();
    Path path = sourceFile.toPath();
    String md5 = hexDigest(Files.newInputStream(path));
    String size = Long.toString(sourceFile.length());

    S3ObjectMetadata returnedObject =
        objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, null,
            ENCODING_GZIP, Files.newInputStream(path), false,
            emptyMap(), null, null, null, emptyList(), Owner.DEFAULT_OWNER);

    assertThat(returnedObject.getKey()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.getContentType()).as(
        "ContentType should be '" + "binary/octet-stream" + "'").isEqualTo("binary/octet-stream");
    assertThat(returnedObject.getContentEncoding()).as(
        "ContentEncoding should be '" + ENCODING_GZIP + "'").isEqualTo(ENCODING_GZIP);
    assertThat(returnedObject.getEtag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    assertThat(returnedObject.getSize()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.isEncrypted()).as("File should not be encrypted!").isFalse();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.getDataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    UUID id = managedId();
    String name = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, ENCODING_GZIP,
            Files.newInputStream(path), false,
            emptyMap(), null, null, null, emptyList(), Owner.DEFAULT_OWNER);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getKey()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.getContentType()).as(
        "ContentType should be '" + TEXT_PLAIN + "'").isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.getContentEncoding()).as(
        "ContentEncoding should be '" + ENCODING_GZIP + "'").isEqualTo(ENCODING_GZIP);
    String md5 = hexDigest(Files.newInputStream(path));
    assertThat(returnedObject.getEtag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    String size = Long.toString(sourceFile.length());
    assertThat(returnedObject.getSize()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.isEncrypted()).as("File should not be encrypted!").isFalse();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.getDataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreObjectEncrypted() {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();
    String contentType = ContentType.TEXT_PLAIN.toString();

    S3ObjectMetadata storedObject =
        objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME),
            id,
            name,
            contentType,
            null,
            new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
            true,
            emptyMap(),
            TEST_ENC_TYPE,
            TEST_ENC_KEY,
            null,
            emptyList(),
            Owner.DEFAULT_OWNER);

    assertThat(storedObject.getSize()).as("File length matches").isEqualTo("36");
    assertThat(storedObject.isEncrypted()).as("File should be encrypted").isTrue();
    assertThat(storedObject.getKmsEncryption()).as("Encryption Type matches")
        .isEqualTo(TEST_ENC_TYPE);
    assertThat(storedObject.getKmsKeyId()).as("Encryption Key matches").isEqualTo(TEST_ENC_KEY);
    String md5 = hexDigest(TEST_ENC_KEY,
        new ByteArrayInputStream(UNSIGNED_CONTENT.getBytes(UTF_8)));
    assertThat(storedObject.getEtag()).as("MD5 should not match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndGetObjectEncrypted() {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();
    String contentType = ContentType.TEXT_PLAIN.toString();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME),
        id,
        name,
        contentType,
        null,
        new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
        true,
        emptyMap(),
        TEST_ENC_TYPE,
        TEST_ENC_KEY,
        null,
        emptyList(),
        Owner.DEFAULT_OWNER);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);
    assertThat(returnedObject.getSize()).as("File length matches").isEqualTo("36");
    assertThat(returnedObject.isEncrypted()).as("File should be encrypted").isTrue();
    assertThat(returnedObject.getKmsEncryption()).as("Encryption Type matches")
        .isEqualTo(TEST_ENC_TYPE);
    assertThat(returnedObject.getKmsKeyId()).as("Encryption Key matches").isEqualTo(TEST_ENC_KEY);
    String md5 = hexDigest(TEST_ENC_KEY,
        new ByteArrayInputStream(UNSIGNED_CONTENT.getBytes(UTF_8)));
    assertThat(returnedObject.getEtag()).as("MD5 should not match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndGetObject_startsWithSlash() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();
    UUID id = managedId();
    String name = "/app/config/" + sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, ENCODING_GZIP,
            Files.newInputStream(path), false,
            emptyMap(), null, null, null, emptyList(), Owner.DEFAULT_OWNER);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getKey()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.getContentType()).as(
        "ContentType should be '" + TEXT_PLAIN + "'").isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.getContentEncoding()).as(
        "ContentEncoding should be '" + ENCODING_GZIP + "'").isEqualTo(ENCODING_GZIP);
    String md5 = hexDigest(Files.newInputStream(path));
    assertThat(returnedObject.getEtag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    String size = Long.toString(sourceFile.length());
    assertThat(returnedObject.getSize()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.isEncrypted()).as("File should not be encrypted!").isFalse();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.getDataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObjectWithTags() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();
    List<Tag> tags = new ArrayList<>();
    tags.add(new Tag("foo", "bar"));

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        ENCODING_GZIP, Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, tags, Owner.DEFAULT_OWNER);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getTags().get(0).getKey()).as("Tag should be present")
        .isEqualTo("foo");
    assertThat(returnedObject.getTags().get(0).getValue()).as("Tag value should be bar")
        .isEqualTo("bar");
  }

  @Test
  void testStoreAndGetTagsOnExistingObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        ENCODING_GZIP,
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);

    List<Tag> tags = new ArrayList<>();
    tags.add(new Tag("foo", "bar"));
    objectStore.storeObjectTags(metadataFrom(TEST_BUCKET_NAME), id, tags);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getTags().get(0).getKey()).as("Tag should be present")
        .isEqualTo("foo");
    assertThat(returnedObject.getTags().get(0).getValue()).as("Tag value should be bar")
        .isEqualTo("bar");
  }

  @Test
  void testStoreAndGetRetentionOnExistingObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        ENCODING_GZIP,
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);

    //TODO: resolution of time seems to matter here. Is this a serialization problem?
    Instant now = Instant.now().truncatedTo(MILLIS);
    Retention retention = new Retention(Mode.COMPLIANCE, now);
    objectStore.storeRetention(metadataFrom(TEST_BUCKET_NAME), id, retention);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getRetention()).isNotNull();
    assertThat(returnedObject.getRetention().getMode()).isEqualTo(Mode.COMPLIANCE);
    assertThat(returnedObject.getRetention().getRetainUntilDate()).isEqualTo(now);
  }

  @Test
  void testStoreAndGetLegalHoldOnExistingObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        ENCODING_GZIP,
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);

    LegalHold legalHold = new LegalHold(LegalHold.Status.ON);
    objectStore.storeLegalHold(metadataFrom(TEST_BUCKET_NAME), id, legalHold);

    S3ObjectMetadata returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.getLegalHold()).isNotNull();
    assertThat(returnedObject.getLegalHold().getStatus()).isEqualTo(LegalHold.Status.ON);
  }

  @Test
  void testStoreAndCopyObject() throws Exception {
    String destinationObjectName = "destinationObject";
    String destinationBucketName = "destinationBucket";
    UUID sourceId = managedId();
    UUID destinationId = managedId();
    File sourceFile = new File(TEST_FILE_PATH);

    String sourceBucketName = "sourceBucket";
    String sourceObjectName = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(sourceBucketName), sourceId, sourceObjectName,
        TEXT_PLAIN, ENCODING_GZIP, Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);

    objectStore.copyS3Object(metadataFrom(sourceBucketName), sourceId,
        metadataFrom(destinationBucketName),
        destinationId, destinationObjectName, NO_ENC, NO_ENC_KEY, NO_USER_METADATA);
    S3ObjectMetadata copiedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId);

    assertThat(copiedObject.isEncrypted()).as("File should not be encrypted!").isFalse();
    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal!").isEqualTo(
        contentOf(copiedObject.getDataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndCopyObjectEncrypted() throws Exception {
    String destinationObjectName = "destinationObject";
    String destinationBucketName = "destinationBucket";
    UUID sourceId = managedId();
    UUID destinationId = managedId();
    File sourceFile = new File(TEST_FILE_PATH);
    Path path = sourceFile.toPath();

    String sourceBucketName = "sourceBucket";
    String sourceObjectName = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(sourceBucketName), sourceId, sourceObjectName,
        TEXT_PLAIN, ENCODING_GZIP, Files.newInputStream(path), false,
        NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);

    objectStore.copyS3Object(metadataFrom(sourceBucketName),
        sourceId,
        metadataFrom(destinationBucketName),
        destinationId,
        destinationObjectName,
        TEST_ENC_TYPE,
        TEST_ENC_KEY,
        NO_USER_METADATA);

    S3ObjectMetadata copiedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId);

    assertThat(copiedObject.isEncrypted()).as("File should be encrypted!").isTrue();
    assertThat(copiedObject.getSize()).as("Files should have the same length").isEqualTo(
        String.valueOf(sourceFile.length()));
    String md5 = hexDigest(TEST_ENC_KEY, Files.newInputStream(path));
    assertThat(copiedObject.getEtag()).as("MD5 should match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndDeleteObject() throws Exception {
    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String objectName = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            ENCODING_GZIP, Files.newInputStream(sourceFile.toPath()), false,
            NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);
    boolean objectDeleted = objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id);
    S3ObjectMetadata s3ObjectMetadata =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(objectDeleted).as("Deletion should succeed!").isTrue();
    assertThat(s3ObjectMetadata).as("Object should be null!").isNull();
  }

  @Test
  void testStoreAndRetrieveAcl() throws IOException {
    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    File sourceFile = new File(TEST_FILE_PATH);
    UUID id = managedId();
    String objectName = sourceFile.getName();
    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            ENCODING_GZIP, Files.newInputStream(sourceFile.toPath()), false,
            NO_USER_METADATA, NO_ENC, NO_ENC_KEY, null, emptyList(), Owner.DEFAULT_OWNER);
    BucketMetadata bucket = metadataFrom(TEST_BUCKET_NAME);
    objectStore.storeAcl(bucket, id, policy);

    AccessControlPolicy actual = objectStore.readAcl(bucket, id);

    assertThat(actual).isEqualTo(policy);
  }

  private UUID managedId() {
    UUID uuid = UUID.randomUUID();
    idCache.add(uuid);
    return uuid;
  }

  /**
   * Deletes all created files from disk.
   */
  @AfterEach
  void cleanupStores() {
    List<UUID> deletedIds = new ArrayList<>();
    for (UUID id : idCache) {
      objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id);
      objectStore.deleteObject(metadataFrom("bucket1"), id);
      objectStore.deleteObject(metadataFrom("bucket2"), id);
      objectStore.deleteObject(metadataFrom("destinationBucket"), id);
      objectStore.deleteObject(metadataFrom("sourceBucket"), id);
      deletedIds.add(id);
    }

    for (UUID id : deletedIds) {
      idCache.remove(id);
    }
  }

  @AfterAll
  static void afterAll() {
    assertThat(idCache).isEmpty();
  }
}
