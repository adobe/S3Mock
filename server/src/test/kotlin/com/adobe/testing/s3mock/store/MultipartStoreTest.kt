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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;

import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.StorageClass;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
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
import org.springframework.http.HttpRange;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = {KmsKeyStore.class, BucketStore.class})
@SpringBootTest(classes = {StoreConfiguration.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Execution(SAME_THREAD)
class MultipartStoreTest extends StoreTestBase {
  private static final List<UUID> idCache = Collections.synchronizedList(new ArrayList<>());

  @Autowired
  private MultipartStore multipartStore;
  @Autowired
  private ObjectStore objectStore;
  @Autowired
  private File rootFolder;

  @BeforeEach
  void beforeEach() {
    assertThat(idCache).isEmpty();
  }

  @Test
  void shouldCreateMultipartUploadFolder() {
    var fileName = "aFile";
    var id = managedId();
    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    var destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(),
                TEST_BUCKET_NAME,
                MultipartStore.MULTIPARTS_FOLDER,
                uploadId)
            .toFile();

    assertThat(destinationFolder)
        .exists()
        .isDirectory();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldCreateMultipartUploadFolderIfBucketExists() {
    var fileName = "aFile";
    var id = managedId();
    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    var destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(),
                TEST_BUCKET_NAME,
                MultipartStore.MULTIPARTS_FOLDER,
                uploadId)
            .toFile();

    assertThat(destinationFolder)
        .exists()
        .isDirectory();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldStorePart() throws IOException {
    var fileName = "PartFile";
    var partNumber = "1";
    var id = managedId();
    var part = "Part1";
    var tempFile = Files.createTempFile("", "");
    new ByteArrayInputStream(part.getBytes())
        .transferTo(Files.newOutputStream(tempFile));
    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    multipartStore.putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, partNumber,
        tempFile, emptyMap());
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(),
            TEST_BUCKET_NAME,
            MultipartStore.MULTIPARTS_FOLDER,
            uploadId,
            partNumber + ".part"
            ).toFile()
    ).exists();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldFinishUpload() throws IOException {
    var fileName = "PartFile";
    var id = managedId();
    var part1 = "Part1";
    var part2 = "Part2";
    var tempFile1 = Files.createTempFile("", "");
    new ByteArrayInputStream(part1.getBytes())
        .transferTo(Files.newOutputStream(tempFile1));
    var tempFile2 = Files.createTempFile("", "");
    new ByteArrayInputStream(part2.getBytes())
        .transferTo(Files.newOutputStream(tempFile2));
    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
            tempFile1, emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2",
            tempFile2, emptyMap());

    var etag =
        multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
            uploadId, getParts(2), emptyMap());
    var allMd5s = ArrayUtils.addAll(
        DigestUtils.md5("Part1"),
        DigestUtils.md5("Part2")
    );

    assertThat(
        Paths.get(rootFolder.getAbsolutePath(),
            TEST_BUCKET_NAME, id.toString(), "binaryData").toFile()).exists();
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(),
            TEST_BUCKET_NAME, id.toString(), "objectMetadata.json").toFile()).exists();
    assertThat(etag).isEqualTo(DigestUtils.md5Hex(allMd5s) + "-2");
  }

  @Test
  void hasValidMetadata() throws IOException {
    var fileName = "PartFile";
    var id = managedId();
    var part1 = "Part1";
    var part2 = "Part2";
    var tempFile1 = Files.createTempFile("", "");
    new ByteArrayInputStream(part1.getBytes())
        .transferTo(Files.newOutputStream(tempFile1));
    var tempFile2 = Files.createTempFile("", "");
    new ByteArrayInputStream(part2.getBytes())
        .transferTo(Files.newOutputStream(tempFile2));

    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1", tempFile1, emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2", tempFile2, emptyMap());

    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
        getParts(2), emptyMap());

    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);
    assertThat(s3ObjectMetadata.size()).isEqualTo("10");
    assertThat(s3ObjectMetadata.contentType()).isEqualTo(APPLICATION_OCTET_STREAM.toString());
  }

  private List<CompletedPart> getParts(int n) {
    var parts = new ArrayList<CompletedPart>();
    for (var i = 1; i <= n; i++) {
      parts.add(new CompletedPart(i, null, null, null, null, null));
    }
    return parts;
  }

  @Test
  void returnsValidPartsFromMultipart() throws IOException {
    var fileName = "PartFile";
    var id = managedId();
    var part1 = "Part1";
    var part2 = "Part2";
    var tempFile1 = Files.createTempFile("", "");
    new ByteArrayInputStream(part1.getBytes())
        .transferTo(Files.newOutputStream(tempFile1));
    var tempFile2 = Files.createTempFile("", "");
    new ByteArrayInputStream(part2.getBytes())
        .transferTo(Files.newOutputStream(tempFile2));

    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();

    multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1", tempFile1,
        emptyMap());
    multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2", tempFile2,
        emptyMap());

    var parts =
        multipartStore.getMultipartUploadParts(metadataFrom(TEST_BUCKET_NAME), id, uploadId);

    assertThat(parts).hasSize(2);
    var expectedPart1 = prepareExpectedPart(1, parts.get(0).lastModified(), part1);
    var expectedPart2 = prepareExpectedPart(2, parts.get(1).lastModified(), part2);

    assertThat(parts.get(0)).isEqualTo(expectedPart1);
    assertThat(parts.get(1)).isEqualTo(expectedPart2);

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  private Part prepareExpectedPart(int partNumber, Date lastModified, String content) {
    return new Part(partNumber,
        DigestUtils.md5Hex(content),
        lastModified,
        (long) content.getBytes().length);
  }

  @Test
  void deletesTemporaryMultipartUploadFolder() throws IOException {
    var fileName = "PartFile";
    var id = managedId();
    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    var tempFile = Files.createTempFile("", "");
    new ByteArrayInputStream("Part1".getBytes())
        .transferTo(Files.newOutputStream(tempFile));
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
            tempFile, emptyMap());

    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
        getParts(1), emptyMap());

    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, uploadId)
            .toFile()
    ).doesNotExist();
  }

  @Test
  void listsMultipartUploads() {
    var bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty();

    var fileName = "PartFile";
    var id = managedId();
    var multipartUpload = multipartStore
        .createMultipartUpload(bucketMetadata, fileName, id, DEFAULT_CONTENT_TYPE, storeHeaders(),
            TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
            StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();

    var uploads =
        multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX);
    assertThat(uploads).hasSize(1);
    var upload = uploads.iterator().next();
    assertThat(upload).isEqualTo(multipartUpload);
    // and some specific sanity checks
    assertThat(upload.uploadId()).isEqualTo(uploadId);
    assertThat(upload.key()).isEqualTo(fileName);

    multipartStore.completeMultipartUpload(bucketMetadata, fileName, id, uploadId, getParts(0),
        emptyMap());

    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty();
  }

  @Test
  void listsMultipartUploadsMultipleBuckets() {
    var bucketName1 = "bucket1";
    BucketMetadata bucketMetadata1 = metadataFrom(bucketName1);
    assertThat(multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX)).isEmpty();
    var bucketName2 = "bucket2";
    BucketMetadata bucketMetadata2 = metadataFrom(bucketName2);
    assertThat(multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX)).isEmpty();

    var fileName1 = "PartFile1";
    var id1 = managedId();
    var multipartUpload1 = multipartStore
        .createMultipartUpload(bucketMetadata1, fileName1, id1, DEFAULT_CONTENT_TYPE,
            storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
            StorageClass.STANDARD, null, null);
    var uploadId1 = multipartUpload1.uploadId();
    var fileName2 = "PartFile2";
    var id2 = managedId();
    var multipartUpload2 = multipartStore
        .createMultipartUpload(bucketMetadata2, fileName2, id2, DEFAULT_CONTENT_TYPE,
            storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
            StorageClass.STANDARD, null, null);
    var uploadId2 = multipartUpload2.uploadId();

    var uploads1 = multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX);
    assertThat(uploads1).hasSize(1);
    var upload1 = uploads1.iterator().next();
    assertThat(upload1).isEqualTo(multipartUpload1);
    // and some specific sanity checks
    assertThat(upload1.uploadId()).isEqualTo(uploadId1);
    assertThat(upload1.key()).isEqualTo(fileName1);

    var uploads2 = multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX);
    assertThat(uploads2).hasSize(1);
    var upload2 = uploads2.iterator().next();
    assertThat(upload2).isEqualTo(multipartUpload2);
    // and some specific sanity checks
    assertThat(upload2.uploadId()).isEqualTo(uploadId2);
    assertThat(upload2.key()).isEqualTo(fileName2);

    multipartStore.completeMultipartUpload(bucketMetadata1, fileName1, id1, uploadId1,
        getParts(0), emptyMap());
    multipartStore.completeMultipartUpload(bucketMetadata2, fileName2, id2, uploadId2,
        getParts(0), emptyMap());

    assertThat(multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX)).isEmpty();
    assertThat(multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX)).isEmpty();
  }

  @Test
  void abortMultipartUpload() throws IOException {
    BucketMetadata bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty();

    var fileName = "PartFile";
    var id = managedId();
    var multipartUpload = multipartStore.createMultipartUpload(
        bucketMetadata, fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();
    var tempFile = Files.createTempFile("", "");
    new ByteArrayInputStream("Part1".getBytes())
        .transferTo(Files.newOutputStream(tempFile));
    multipartStore.putPart(bucketMetadata, id, uploadId, "1",
        tempFile, emptyMap());
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).hasSize(1);

    multipartStore.abortMultipartUpload(bucketMetadata, id, uploadId);

    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty();
    assertThat(Paths.get(rootFolder.getAbsolutePath(),
        TEST_BUCKET_NAME, fileName, "binaryData").toFile()).doesNotExist();
    assertThat(Paths.get(rootFolder.getAbsolutePath(),
        TEST_BUCKET_NAME, fileName, "objectMetadata.json").toFile()).doesNotExist();
    assertThat(Paths.get(rootFolder.getAbsolutePath(),
        TEST_BUCKET_NAME, fileName, uploadId).toFile()).doesNotExist();
  }

  @Test
  void copyPart() throws IOException {
    var sourceFile = UUID.randomUUID().toString();
    var sourceId = managedId();
    var targetFile = UUID.randomUUID().toString();
    final var partNumber = "1";
    var destinationId = managedId();

    var contentBytes = UUID.randomUUID().toString().getBytes();
    var tempFile = Files.createTempFile("", "");
    new ByteArrayInputStream(contentBytes)
        .transferTo(Files.newOutputStream(tempFile));
    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), sourceId, sourceFile,
        DEFAULT_CONTENT_TYPE, storeHeaders(), tempFile,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), targetFile, destinationId,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();

    var range = HttpRange.createByteRange(0, contentBytes.length);
    multipartStore.copyPart(
        metadataFrom(TEST_BUCKET_NAME), sourceId, range, partNumber,
        metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId, emptyMap());
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(),
                TEST_BUCKET_NAME,
                MultipartStore.MULTIPARTS_FOLDER,
                uploadId, partNumber + ".part")
            .toFile()
    ).exists();
    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId);
  }

  @Test
  void copyPartNoRange() throws IOException {
    var sourceFile = UUID.randomUUID().toString();
    var sourceId = managedId();
    var targetFile = UUID.randomUUID().toString();
    final var partNumber = "1";
    var destinationId = managedId();
    var contentBytes = UUID.randomUUID().toString().getBytes();
    var bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    var tempFile = Files.createTempFile("", "");
    new ByteArrayInputStream(contentBytes)
        .transferTo(Files.newOutputStream(tempFile));
    objectStore.storeS3ObjectMetadata(bucketMetadata, sourceId, sourceFile, DEFAULT_CONTENT_TYPE,
        storeHeaders(), tempFile,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD);

    var multipartUpload = multipartStore.createMultipartUpload(
        bucketMetadata, targetFile, destinationId,
        DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap(), StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();

    multipartStore.copyPart(
        bucketMetadata, sourceId, null, partNumber,
        bucketMetadata, destinationId, uploadId, emptyMap());

    assertThat(
        Paths.get(bucketMetadata.path().toString(),
            MultipartStore.MULTIPARTS_FOLDER,
            uploadId,
            partNumber + ".part").toFile()
    ).exists();
    multipartStore.abortMultipartUpload(bucketMetadata, destinationId, uploadId);
  }

  @Test
  void missingUploadPreparation() {
    var range = HttpRange.createByteRange(0, 0);
    var id = UUID.randomUUID();
    var destinationId = UUID.randomUUID();
    var uploadId = UUID.randomUUID().toString();
    var bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    var encryptionHeaders = encryptionHeaders();
    var e = assertThrows(IllegalStateException.class, () ->
        multipartStore.copyPart(
            bucketMetadata, id, range, "1",
            bucketMetadata, destinationId, uploadId,
            encryptionHeaders)
    );

    assertThat(e.getMessage()).startsWith("Multipart Request was not prepared.");
  }

  @Test
  void multipartUploadPartsAreSortedNumerically() throws IOException {
    var filename = UUID.randomUUID().toString();
    var id = managedId();

    var multipartUpload = multipartStore.createMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), filename, id, TEXT_PLAIN,
        storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
        StorageClass.STANDARD, null, null);
    var uploadId = multipartUpload.uploadId();

    for (int i = 1; i < 11; i++) {
      Path tempFile = Files.createTempFile("", "");
      new ByteArrayInputStream((i + "\n").getBytes(UTF_8))
          .transferTo(Files.newOutputStream(tempFile));

      multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, String.valueOf(i),
          tempFile, emptyMap());
    }
    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), filename, id, uploadId,
        getParts(10), emptyMap());
    var s = FileUtils
        .readLines(objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id)
            .dataPath().toFile(), "UTF8");

    assertThat(s).contains(rangeClosed(1, 10).mapToObj(Integer::toString)
        .toList().toArray(new String[]{}));
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
