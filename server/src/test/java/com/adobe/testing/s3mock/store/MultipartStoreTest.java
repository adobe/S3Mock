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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;

import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
@SpringBootTest(classes = {StoreConfiguration.class})
@Execution(SAME_THREAD)
class MultipartStoreTest extends StoreTestBase {
  private static final String ALL_BUCKETS = null;
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
    String fileName = "aFile";
    String uploadId = "12345";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());
    final File destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, id.toString(), uploadId)
            .toFile();

    assertThat(destinationFolder)
        .exists()
        .isDirectory();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldCreateMultipartUploadFolderIfBucketExists() {
    String uploadId = "12345";
    String fileName = "aFile";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());

    final File destinationFolder =
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, id.toString(), uploadId)
            .toFile();

    assertThat(destinationFolder)
        .exists()
        .isDirectory();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldStorePart() {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    final String partNumber = "1";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());

    multipartStore.putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, partNumber,
        new ByteArrayInputStream("Test".getBytes()), false, emptyMap());
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, id.toString(), uploadId,
                partNumber + ".part")
            .toFile()).as("Part does not exist!").exists();

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  @Test
  void shouldFinishUpload() {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false, emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2",
            new ByteArrayInputStream("Part2".getBytes()), false, emptyMap());

    final String etag =
        multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
            uploadId, getParts(2), emptyMap());
    final byte[] allMd5s = ArrayUtils.addAll(
        DigestUtils.md5("Part1"),
        DigestUtils.md5("Part2")
    );

    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, id.toString(),
                "binaryData").toFile()).as("File does not exist!").exists();
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, id.toString(),
                "objectMetadata.json").toFile()).as("Metadata does not exist!").exists();
    assertThat(etag).as("Special etag doesn't match.")
        .isEqualTo(DigestUtils.md5Hex(allMd5s) + "-2");
  }

  @Test
  void hasValidMetadata() {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false, emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2",
            new ByteArrayInputStream("Part2".getBytes()), false, emptyMap());

    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
        getParts(2), emptyMap());

    final S3ObjectMetadata s3ObjectMetadata =
        objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id);
    assertThat(s3ObjectMetadata.getSize()).as("Size doesn't match.").isEqualTo("10");
    assertThat(s3ObjectMetadata.getContentType()).isEqualTo(APPLICATION_OCTET_STREAM.toString());
  }

  private List<CompletedPart> getParts(int n) {
    List<CompletedPart> parts = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      parts.add(new CompletedPart(i, null));
    }
    return parts;
  }

  @Test
  void returnsValidPartsFromMultipart() {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    String part1 = "Part1";
    ByteArrayInputStream part1Stream = new ByteArrayInputStream(part1.getBytes());
    String part2 = "Part2";
    ByteArrayInputStream part2Stream = new ByteArrayInputStream(part2.getBytes());

    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());

    multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1", part1Stream, false,
        emptyMap());
    multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2", part2Stream, false,
        emptyMap());

    List<Part> parts =
        multipartStore.getMultipartUploadParts(metadataFrom(TEST_BUCKET_NAME), id, uploadId);

    assertThat(parts).as("Part quantity does not match").hasSize(2);

    var expectedPart1 = prepareExpectedPart(1, parts.get(0).lastModified(), part1);
    var expectedPart2 = prepareExpectedPart(2, parts.get(1).lastModified(), part2);

    assertThat(parts.get(0)).as("Part 1 attributes doesn't match").isEqualTo(expectedPart1);
    assertThat(parts.get(1)).as("Part 2 attributes doesn't match").isEqualTo(expectedPart2);

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);
  }

  private Part prepareExpectedPart(int partNumber, Date lastModified, String content) {
    return new Part(partNumber,
        DigestUtils.md5Hex(content),
        lastModified,
        (long) content.getBytes().length);
  }

  @Test
  void deletesTemporaryMultipartUploadFolder() {
    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());
    multipartStore
        .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
            new ByteArrayInputStream("Part1".getBytes()), false, emptyMap());

    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
        getParts(1), emptyMap());

    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, uploadId)
            .toFile()).as("Folder should not exist anymore!").doesNotExist();
  }

  @Test
  void listsMultipartUploads() {
    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();

    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    BucketMetadata bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    final MultipartUpload initiatedUpload = multipartStore
        .prepareMultipartUpload(bucketMetadata, fileName, id, DEFAULT_CONTENT_TYPE, storeHeaders(),
            uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap());

    final Collection<MultipartUpload> uploads =
        multipartStore.listMultipartUploads(TEST_BUCKET_NAME, NO_PREFIX);
    assertThat(uploads).hasSize(1);
    final MultipartUpload upload = uploads.iterator().next();
    assertThat(upload).isEqualTo(initiatedUpload);
    // and some specific sanity checks
    assertThat(upload.uploadId()).isEqualTo(uploadId);
    assertThat(upload.key()).isEqualTo(fileName);

    multipartStore.completeMultipartUpload(bucketMetadata, fileName, id, uploadId, getParts(0),
        emptyMap());

    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();
  }

  @Test
  void listsMultipartUploadsMultipleBuckets() {
    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();

    final String fileName1 = "PartFile1";
    final String uploadId1 = "123451";
    final String bucketName1 = "bucket1";
    UUID id1 = managedId();
    final MultipartUpload initiatedUpload1 = multipartStore
        .prepareMultipartUpload(metadataFrom(bucketName1), fileName1, id1, DEFAULT_CONTENT_TYPE,
            storeHeaders(), uploadId1, TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap());
    final String fileName2 = "PartFile2";
    final String uploadId2 = "123452";
    final String bucketName2 = "bucket2";
    UUID id2 = managedId();
    final MultipartUpload initiatedUpload2 = multipartStore
        .prepareMultipartUpload(metadataFrom(bucketName2), fileName2, id2, DEFAULT_CONTENT_TYPE,
            storeHeaders(), uploadId2, TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap());

    final Collection<MultipartUpload> uploads1 = multipartStore.listMultipartUploads(bucketName1,
        NO_PREFIX);
    assertThat(uploads1).hasSize(1);
    final MultipartUpload upload1 = uploads1.iterator().next();
    assertThat(upload1).isEqualTo(initiatedUpload1);
    // and some specific sanity checks
    assertThat(upload1.uploadId()).isEqualTo(uploadId1);
    assertThat(upload1.key()).isEqualTo(fileName1);

    final Collection<MultipartUpload> uploads2 = multipartStore.listMultipartUploads(bucketName2,
        NO_PREFIX);
    assertThat(uploads2).hasSize(1);
    final MultipartUpload upload2 = uploads2.iterator().next();
    assertThat(upload2).isEqualTo(initiatedUpload2);
    // and some specific sanity checks
    assertThat(upload2.uploadId()).isEqualTo(uploadId2);
    assertThat(upload2.key()).isEqualTo(fileName2);

    multipartStore.completeMultipartUpload(metadataFrom(bucketName1), fileName1, id1, uploadId1,
        getParts(0), emptyMap());
    multipartStore.completeMultipartUpload(metadataFrom(bucketName2), fileName2, id2, uploadId2,
        getParts(0), emptyMap());

    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();
  }

  @Test
  void abortMultipartUpload() {
    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();

    final String fileName = "PartFile";
    final String uploadId = "12345";
    UUID id = managedId();
    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), fileName, id,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());
    multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
        new ByteArrayInputStream("Part1".getBytes()), false, emptyMap());
    assertThat(multipartStore.listMultipartUploads(TEST_BUCKET_NAME, NO_PREFIX)).hasSize(1);

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId);

    assertThat(multipartStore.listMultipartUploads(ALL_BUCKETS, NO_PREFIX)).isEmpty();
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName,
                "binaryData").toFile()).as("File exists!").doesNotExist();
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName,
                "objectMetadata").toFile()).as("Metadata exists!").doesNotExist();
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, fileName, uploadId)
            .toFile()).as("Temp upload folder exists!").doesNotExist();
  }

  @Test
  void copyPart() {
    final String sourceFile = UUID.randomUUID().toString();
    final String uploadId = UUID.randomUUID().toString();
    UUID sourceId = managedId();
    final String targetFile = UUID.randomUUID().toString();
    final String partNumber = "1";
    UUID destinationId = managedId();

    final byte[] contentBytes = UUID.randomUUID().toString().getBytes();
    objectStore.storeS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), sourceId, sourceFile,
        DEFAULT_CONTENT_TYPE, storeHeaders(), new ByteArrayInputStream(contentBytes), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), targetFile, destinationId,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());

    HttpRange range = HttpRange.createByteRange(0, contentBytes.length);
    multipartStore.copyPart(
        metadataFrom(TEST_BUCKET_NAME), sourceId, range, partNumber,
        metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId, emptyMap());
    assertThat(
        Paths.get(rootFolder.getAbsolutePath(), TEST_BUCKET_NAME, destinationId.toString(),
                uploadId, partNumber + ".part")
            .toFile()).as("Part does not exist!").exists();
    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId);
  }

  @Test
  void copyPartNoRange() {
    final String sourceFile = UUID.randomUUID().toString();
    final String uploadId = UUID.randomUUID().toString();
    UUID sourceId = managedId();
    final String targetFile = UUID.randomUUID().toString();
    final String partNumber = "1";
    UUID destinationId = managedId();
    final byte[] contentBytes = UUID.randomUUID().toString().getBytes();
    BucketMetadata bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    objectStore.storeS3ObjectMetadata(bucketMetadata, sourceId, sourceFile, DEFAULT_CONTENT_TYPE,
        storeHeaders(), new ByteArrayInputStream(contentBytes), false,
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER);

    multipartStore.prepareMultipartUpload(bucketMetadata, targetFile, destinationId,
        DEFAULT_CONTENT_TYPE, storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
        emptyMap());

    multipartStore.copyPart(
        bucketMetadata, sourceId, null, partNumber,
        bucketMetadata, destinationId, uploadId, emptyMap());

    assertThat(
        Paths.get(bucketMetadata.path().toString(), destinationId.toString(),
                uploadId, partNumber + ".part")
            .toFile()).as("Part does not exist!").exists();
    multipartStore.abortMultipartUpload(bucketMetadata, destinationId, uploadId);
  }

  @Test
  void missingUploadPreparation() {
    HttpRange range = HttpRange.createByteRange(0, 0);
    IllegalStateException e = assertThrows(IllegalStateException.class, () ->
        multipartStore.copyPart(
            metadataFrom(TEST_BUCKET_NAME), UUID.randomUUID(), range, "1",
            metadataFrom(TEST_BUCKET_NAME), UUID.randomUUID(), UUID.randomUUID().toString(),
            emptyMap())
    );

    assertThat(e.getMessage()).isEqualTo("Missed preparing Multipart Request.");
  }

  @Test
  void multipartUploadPartsAreSortedNumerically() throws IOException {
    final String uploadId = UUID.randomUUID().toString();
    final String filename = UUID.randomUUID().toString();
    UUID id = managedId();

    multipartStore.prepareMultipartUpload(metadataFrom(TEST_BUCKET_NAME), filename, id, TEXT_PLAIN,
        storeHeaders(), uploadId, TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap());
    for (int i = 1; i < 11; i++) {
      final ByteArrayInputStream inputStream = new ByteArrayInputStream((i + "\n").getBytes());

      multipartStore.putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, String.valueOf(i),
          inputStream, false, emptyMap());
    }
    multipartStore.completeMultipartUpload(metadataFrom(TEST_BUCKET_NAME), filename, id, uploadId,
        getParts(10), emptyMap());
    final List<String> s = FileUtils
        .readLines(objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id)
                .getDataPath().toFile(), "UTF8");

    assertThat(s).contains(rangeClosed(1, 10).mapToObj(Integer::toString)
        .collect(toList()).toArray(new String[] {}));
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
