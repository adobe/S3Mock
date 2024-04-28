/*
 *  Copyright 2017-2024 Adobe.
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
import com.adobe.testing.s3mock.dto.CanonicalUser;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

    var returnedObject =
        objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, null,
            storeHeaders(), path,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
            StorageClass.STANDARD);

    assertThat(returnedObject.key()).isEqualTo(name);
    assertThat(returnedObject.contentType()).isEqualTo(DEFAULT_CONTENT_TYPE);
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    assertThat(returnedObject.etag())
        .isEqualTo("\"" + hexDigest(Files.newInputStream(path)) + "\"");
    assertThat(returnedObject.size()).isEqualTo(Long.toString(sourceFile.length()));
    assertThat(returnedObject.encryptionHeaders()).isEmpty();
    assertThat(contentOf(sourceFile, UTF_8))
        .isEqualTo(contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObject() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var id = managedId();
    var name = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
            path,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
            StorageClass.DEEP_ARCHIVE);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.key()).isEqualTo(name);
    assertThat(returnedObject.contentType()).isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    assertThat(returnedObject.etag())
        .isEqualTo("\"" + hexDigest(Files.newInputStream(path)) + "\"");
    assertThat(returnedObject.size()).isEqualTo(Long.toString(sourceFile.length()));
    assertThat(returnedObject.encryptionHeaders()).isEmpty();
    assertThat(returnedObject.storageClass()).isEqualTo(StorageClass.DEEP_ARCHIVE);

    assertThat(contentOf(sourceFile, UTF_8))
        .isEqualTo(contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObject_startsWithSlash() throws Exception {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var id = managedId();
    var name = "/app/config/" + sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
            path,
            emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
            StorageClass.STANDARD);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.key()).isEqualTo(name);
    assertThat(returnedObject.contentType()).isEqualTo(TEXT_PLAIN);
    assertThat(returnedObject.storeHeaders()).containsEntry(CONTENT_ENCODING, ENCODING_GZIP);
    assertThat(returnedObject.etag())
        .isEqualTo("\"" + hexDigest(Files.newInputStream(path)) + "\"");
    assertThat(returnedObject.size()).isEqualTo(Long.toString(sourceFile.length()));
    assertThat(returnedObject.encryptionHeaders()).isEmpty();

    assertThat(contentOf(sourceFile, UTF_8))
        .isEqualTo(contentOf(returnedObject.dataPath().toFile(), UTF_8));
  }

  @Test
  void testStoreAndGetObjectWithTags() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();
    var tags = List.of(new Tag("foo", "bar"));

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, tags, null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.tags().get(0).key()).isEqualTo("foo");
    assertThat(returnedObject.tags().get(0).value()).isEqualTo("bar");
  }

  @Test
  void testStoreAndGetTagsOnExistingObject() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    objectStore.storeObjectTags(metadataFrom(TEST_BUCKET_NAME), id, List.of(new Tag("foo", "bar")));
    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.tags().get(0).key()).isEqualTo("foo");
    assertThat(returnedObject.tags().get(0).value()).isEqualTo("bar");
  }

  @Test
  void testStoreAndGetRetentionOnExistingObject() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    //TODO: resolution of time seems to matter here. Is this a serialization problem?
    var now = Instant.now().truncatedTo(MILLIS);
    var retention = new Retention(Mode.COMPLIANCE, now);
    objectStore.storeRetention(metadataFrom(TEST_BUCKET_NAME), id, retention);

    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.retention()).isNotNull();
    assertThat(returnedObject.retention().mode()).isEqualTo(Mode.COMPLIANCE);
    assertThat(returnedObject.retention().retainUntilDate()).isEqualTo(now);
  }

  @Test
  void testStoreAndGetLegalHoldOnExistingObject() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var name = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    var legalHold = new LegalHold(LegalHold.Status.ON);
    objectStore.storeLegalHold(metadataFrom(TEST_BUCKET_NAME), id, legalHold);
    var returnedObject = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(returnedObject.legalHold()).isNotNull();
    assertThat(returnedObject.legalHold().status()).isEqualTo(LegalHold.Status.ON);
  }

  @Test
  void testStoreAndCopyObject() {
    var destinationObjectName = "destinationObject";
    var destinationBucketName = "destinationBucket";
    var sourceId = managedId();
    var destinationId = managedId();
    var sourceFile = new File(TEST_FILE_PATH);

    var sourceBucketName = "sourceBucket";
    var sourceObjectName = sourceFile.getName();

    objectStore.storeS3ObjectMetadata(metadataFrom(sourceBucketName), sourceId, sourceObjectName,
        TEXT_PLAIN, storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.GLACIER);

    objectStore.copyS3Object(metadataFrom(sourceBucketName), sourceId,
        metadataFrom(destinationBucketName),
        destinationId, destinationObjectName, emptyMap(), NO_USER_METADATA);
    var copiedObject =
        objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId);

    assertThat(copiedObject.encryptionHeaders()).isEmpty();
    assertThat(contentOf(sourceFile, UTF_8))
        .isEqualTo(contentOf(copiedObject.dataPath().toFile(), UTF_8));
    assertThat(copiedObject.storageClass()).isEqualTo(StorageClass.GLACIER);
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
        TEXT_PLAIN, storeHeaders(), path,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

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
    assertThat(copiedObject.size()).isEqualTo(String.valueOf(sourceFile.length()));
    assertThat(copiedObject.etag())
        .isEqualTo("\"" + hexDigest(TEST_ENC_KEY, Files.newInputStream(path)) + "\"");
  }

  @Test
  void testStoreAndDeleteObject() {
    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var objectName = sourceFile.getName();

    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            storeHeaders(), sourceFile.toPath(),
            NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
            StorageClass.STANDARD);
    var objectDeleted = objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id);
    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);

    assertThat(objectDeleted).isTrue();
    assertThat(s3ObjectMetadata).isNull();
  }

  @Test
  void testStoreAndRetrieveAcl() {
    var owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    var grantee = new CanonicalUser(owner.id(), owner.displayName(), null, null);
    var policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    var sourceFile = new File(TEST_FILE_PATH);
    var id = managedId();
    var objectName = sourceFile.getName();
    objectStore
        .storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
            storeHeaders(), sourceFile.toPath(),
            NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
            StorageClass.STANDARD);
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
