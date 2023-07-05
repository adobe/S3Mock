/*
 *  Copyright 2017-2023 Adobe.
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
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Files.contentOf;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
@SpringBootTest(classes = {StoreConfiguration.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Execution(SAME_THREAD)
class ObjectStoreTest extends StoreTestBase {
  private static final String SIGNED_CONTENT =
      """
          24;chunk-signature=11707b33deb094881a16c70e9cbd5d79053a0bb235c25674e3cf0fed601683b5\r
          ## sample test file ##

          demo=content
          0;chunk-signature=2206490f19c068b46367173d1e155b597fd367037fa3f924290b41c1e83c1c08
          """;
  private static final String UNSIGNED_CONTENT =
          """
                  ## sample test file ##

                  demo=content""";
  private static final List<UUID> idCache = Collections.synchronizedList(new ArrayList<>());

  @Autowired
  private ObjectStore objectStore;

  @BeforeEach
  void beforeEach() {
    assertThat(idCache).isEmpty();
  }

  @Test
  void testStoreObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();
    var path = sourceFile.toPath();
    var md5 = hexDigest(Files.newInputStream(path));
    var size = Long.toString(sourceFile.length());

    var returnedObject =
        objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, null,
            storeHeaders(), Files.newInputStream(path), false,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    assertThat(returnedObject.key()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.contentType()).as(
        "ContentType should be '" + "binary/octet-stream" + "'").isEqualTo("binary/octet-stream");
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    assertThat(returnedObject.etag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    assertThat(returnedObject.size()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.encryptionHeaders()).isEmpty();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var id = managedId();
    var name = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
            Files.newInputStream(path), false,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.key()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.contentType()).as(
        "ContentType should be '" + TEXT_PLAIN + "'").isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    var md5 = hexDigest(Files.newInputStream(path));
    assertThat(returnedObject.etag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    var size = Long.toString(sourceFile.length());
    assertThat(returnedObject.size()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.encryptionHeaders()).isEmpty();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreObjectEncrypted() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();
    var contentType = ContentType.TEXT_PLAIN.toString();

    var storedObject =
        objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME),
            id,
            name,
            contentType,
            null,
            new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
            true,
            emptyMap(),
            encryptionHeaders(),
            null,
            emptyList(),
            null,
            null,
            Owner.DEFAULT_OWNER);

    assertThat(storedObject.size()).as("File length matches").isEqualTo("36");
    assertThat(storedObject.encryptionHeaders()).isEqualTo(encryptionHeaders());
    var md5 = hexDigest(TEST_ENC_KEY,
        new ByteArrayInputStream(UNSIGNED_CONTENT.getBytes(UTF_8)));
    assertThat(storedObject.etag()).as("MD5 should not match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndGetObjectEncrypted() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();
    var contentType = ContentType.TEXT_PLAIN.toString();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME),
        id,
        name,
        contentType,
        null,
        new ByteArrayInputStream(SIGNED_CONTENT.getBytes(UTF_8)),
        true,
        emptyMap(),
        encryptionHeaders(),
        null,
        emptyList(),
        null,
        null,
        Owner.DEFAULT_OWNER);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);
    assertThat(returnedObject.size()).as("File length matches").isEqualTo("36");
    assertThat(returnedObject.encryptionHeaders()).isEqualTo(encryptionHeaders());
    var md5 = hexDigest(TEST_ENC_KEY,
        new ByteArrayInputStream(UNSIGNED_CONTENT.getBytes(UTF_8)));
    assertThat(returnedObject.etag()).as("MD5 should not match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndGetObject_startsWithSlash() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var id = managedId();
    var name = "/app/config/" + sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
            Files.newInputStream(path), false,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.key()).as("Name should be '" + name + "'").isEqualTo(name);
    assertThat(returnedObject.contentType()).as(
        "ContentType should be '" + TEXT_PLAIN + "'").isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    var md5 = hexDigest(Files.newInputStream(path));
    assertThat(returnedObject.etag()).as("MD5 should be '" + md5 + "'")
        .isEqualTo("\"" + md5 + "\"");
    var size = Long.toString(sourceFile.length());
    assertThat(returnedObject.size()).as("Size should be '" + size + "'").isEqualTo(size);
    assertThat(returnedObject.encryptionHeaders()).isEmpty();

    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal").isEqualTo(
        contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObjectWithTags() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();
    var tags = List.of(new Tag("foo", "bar"));

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(), Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, emptyMap(), null, tags, null, null, Owner.DEFAULT_OWNER);

    var returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.tags().get(0).key()).as("Tag should be present")
        .isEqualTo("foo");
    assertThat(returnedObject.tags().get(0).value()).as("Tag value should be bar")
        .isEqualTo("bar");
  }

  @Test
  void testStoreAndGetTagsOnExistingObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(),
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    var tags = List.of(new Tag("foo", "bar"));
    objectStore.storeObjectTags(metadataFrom(TEST_BUCKET_NAME), id, tags);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.tags().get(0).key()).as("Tag should be present")
        .isEqualTo("foo");
    assertThat(returnedObject.tags().get(0).value()).as("Tag value should be bar")
        .isEqualTo("bar");
  }

  @Test
  void testStoreAndGetRetentionOnExistingObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(),
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    //TODO: resolution of time seems to matter here. Is this a serialization problem?
    var now = Instant.now().truncatedTo(MILLIS);
    var retention = new Retention(Mode.COMPLIANCE, now);
    objectStore.storeRetention(metadataFrom(TEST_BUCKET_NAME), id, retention);

    var returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.retention()).isNotNull();
    assertThat(returnedObject.retention().mode()).isEqualTo(Mode.COMPLIANCE);
    assertThat(returnedObject.retention().retainUntilDate()).isEqualTo(now);
  }

  @Test
  void testStoreAndGetLegalHoldOnExistingObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(),
        Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    var legalHold = new LegalHold(LegalHold.Status.ON);
    objectStore.storeLegalHold(metadataFrom(TEST_BUCKET_NAME), id, legalHold);

    var returnedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.legalHold()).isNotNull();
    assertThat(returnedObject.legalHold().status()).isEqualTo(LegalHold.Status.ON);
  }

  @Test
  void testStoreAndCopyObject() throws Exception {
    var destinationObjectName = "destinationObject";
    var destinationBucketName = "destinationBucket";
    var sourceId = managedId();
    var destinationId = managedId();
    var sourceFile = new File(TEST_FILE_PATH);

    var sourceBucketName = "sourceBucket";
    var sourceObjectName = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(sourceBucketName), sourceId, sourceObjectName,
        TEXT_PLAIN, storeHeaders(), Files.newInputStream(sourceFile.toPath()), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    objectStore.copyS3Object(metadataFrom(sourceBucketName), sourceId,
        metadataFrom(destinationBucketName),
        destinationId, destinationObjectName, emptyMap(), NO_USER_METADATA);
    var copiedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId);

    assertThat(copiedObject.encryptionHeaders()).isEmpty();
    assertThat(contentOf(sourceFile, UTF_8)).as("Files should be equal!").isEqualTo(
        contentOf(copiedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndCopyObjectEncrypted() throws Exception {
    var destinationObjectName = "destinationObject";
    var destinationBucketName = "destinationBucket";
    var sourceId = managedId();
    var destinationId = managedId();
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();

    var sourceBucketName = "sourceBucket";
    var sourceObjectName = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(sourceBucketName), sourceId, sourceObjectName,
        TEXT_PLAIN, storeHeaders(), Files.newInputStream(path), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    objectStore.copyS3Object(metadataFrom(sourceBucketName),
        sourceId,
        metadataFrom(destinationBucketName),
        destinationId,
        destinationObjectName,
        encryptionHeaders(),
        NO_USER_METADATA);

    var copiedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId);

    assertThat(copiedObject.encryptionHeaders()).isEqualTo(encryptionHeaders());
    assertThat(copiedObject.size()).as("Files should have the same length").isEqualTo(
        String.valueOf(sourceFile.length()));
    var md5 = hexDigest(TEST_ENC_KEY, Files.newInputStream(path));
    assertThat(copiedObject.etag()).as("MD5 should match").isEqualTo("\"" + md5 + "\"");
  }

  @Test
  void testStoreAndDeleteObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var objectName = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            storeHeaders(), Files.newInputStream(sourceFile.toPath()), false,
            NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);
    var objectDeleted = objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id);
    var s3ObjectMetadata =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(objectDeleted).as("Deletion should succeed!").isTrue();
    assertThat(s3ObjectMetadata).as("Object should be null!").isNull();
  }

  @Test
  void testStoreAndRetrieveAcl() throws IOException {
    var owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    var grantee = Grantee.from(owner);
    var policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var objectName = sourceFile.getName();
    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            storeHeaders(), Files.newInputStream(sourceFile.toPath()), false,
            NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);
    var bucket = metadataFrom(TEST_BUCKET_NAME);
    objectStore.storeAcl(bucket, id, policy);

    var actual = objectStore.readAcl(bucket, id);

    assertThat(actual).isEqualTo(policy);
  }

  private Map<String, String> encryptionHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION, TEST_ENC_TYPE);
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, TEST_ENC_KEY);
    return headers;
  }

  private Map<String, String> storeHeaders() {
    Map<String, String> storeHeaders = new HashMap<>();
    storeHeaders.put(CONTENT_ENCODING, ENCODING_GZIP);
    return storeHeaders;
  }

  private UUID managedId() {
    var uuid = UUID.randomUUID();
    idCache.add(uuid);
    return uuid;
  }

  /**
   * Deletes all created files from disk.
   */
  @AfterEach
  void cleanupStores() {
    var deletedIds = new ArrayList<UUID>();
    for (var id : idCache) {
      objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id);
      objectStore.deleteObject(metadataFrom("bucket1"), id);
      objectStore.deleteObject(metadataFrom("bucket2"), id);
      objectStore.deleteObject(metadataFrom("destinationBucket"), id);
      objectStore.deleteObject(metadataFrom("sourceBucket"), id);
      deletedIds.add(id);
    }

    for (var id : deletedIds) {
      idCache.remove(id);
    }
  }

  @AfterAll
  static void afterAll() {
    assertThat(idCache).isEmpty();
  }
}
