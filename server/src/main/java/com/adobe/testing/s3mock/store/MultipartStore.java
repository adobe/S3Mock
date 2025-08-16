/*
 *  Copyright 2017-2025 Adobe.
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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigestMultipart;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.commons.io.FileUtils.openInputStream;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.stream.Streams;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRange;

public class MultipartStore extends StoreBase {
  private static final Logger LOG = LoggerFactory.getLogger(MultipartStore.class);
  private static final String PART_SUFFIX = ".part";
  private static final String MULTIPART_UPLOAD_META_FILE = "multipartMetadata.json";
  static final String MULTIPARTS_FOLDER = "multiparts";

  /**
   * This map stores one lock object per MultipartUpload ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private final Map<UUID, Object> lockStore = new ConcurrentHashMap<>();
  private final ObjectStore objectStore;
  private final ObjectMapper objectMapper;

  public MultipartStore(ObjectStore objectStore, ObjectMapper objectMapper) {
    this.objectStore = objectStore;
    this.objectMapper = objectMapper;
  }

  public MultipartUpload createMultipartUpload(
      BucketMetadata bucket,
      String key,
      UUID id,
      String contentType,
      Map<String, String> storeHeaders,
      Owner owner,
      Owner initiator,
      Map<String, String> userMetadata,
      Map<String, String> encryptionHeaders,
      List<Tag> tags,
      StorageClass storageClass,
      @Nullable ChecksumType checksumType,
      @Nullable ChecksumAlgorithm checksumAlgorithm) {
    var uploadId = UUID.randomUUID();
    if (!createPartsFolder(bucket, uploadId)) {
      LOG.error("Directories for storing multipart uploads couldn't be created. bucket={}, key={}, "
              + "id={}, uploadId={}", bucket, key, id, uploadId);
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    var upload = new MultipartUpload(checksumAlgorithm,
        ChecksumType.FULL_OBJECT,
        new Date(),
        initiator,
        key,
        owner,
        storageClass,
        uploadId.toString()
    );
    var multipartUploadInfo = new MultipartUploadInfo(upload,
        contentType,
        userMetadata,
        storeHeaders,
        encryptionHeaders,
        bucket.name(),
        storageClass,
        tags,
        null,
        checksumType,
        checksumAlgorithm);
    lockStore.putIfAbsent(uploadId, new Object());
    writeMetafile(bucket, multipartUploadInfo);

    return upload;
  }

  public List<MultipartUpload> listMultipartUploads(BucketMetadata bucketMetadata, @Nullable String prefix) {
    var multipartsFolder = getMultipartsFolder(bucketMetadata);
    if (!multipartsFolder.toFile().exists()) {
      return Collections.emptyList();
    }
    try (var paths = Files.newDirectoryStream(multipartsFolder)) {
      return Streams
          .of(paths)
          .map(
              path -> {
                var fileName = path.getFileName().toString();
                var uploadMetadata = getUploadMetadata(bucketMetadata, UUID.fromString(fileName));
                if (uploadMetadata != null) {
                  return uploadMetadata.upload();
                } else  {
                  return null;
                }
              }
          )
          .filter(Objects::nonNull)
          .filter(multipartUpload -> isBlank(prefix) || multipartUpload.key().startsWith(prefix))
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException("Could not load buckets from data directory ", e);
    }
  }

  @Nullable
  public MultipartUploadInfo getMultipartUploadInfo(
      BucketMetadata bucketMetadata,
      UUID uploadId) {
    return getUploadMetadata(bucketMetadata, uploadId);
  }

  public MultipartUpload getMultipartUpload(BucketMetadata bucketMetadata, UUID uploadId) {
    var uploadMetadata = getUploadMetadata(bucketMetadata, uploadId);
    if (uploadMetadata != null) {
      return uploadMetadata.upload();
    } else {
      throw new IllegalArgumentException("No MultipartUpload found with uploadId: " + uploadId);
    }
  }

  public void abortMultipartUpload(BucketMetadata bucket, UUID id, UUID uploadId) {
    var multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId);
    if (multipartUploadInfo != null) {
      synchronized (lockStore.get(uploadId)) {
        try {
          FileUtils.deleteDirectory(getPartsFolder(bucket, uploadId).toFile());
        } catch (IOException e) {
          throw new IllegalStateException("Could not delete multipart-directory " + uploadId, e);
        }
        lockStore.remove(uploadId);
      }
    }
  }

  public String putPart(
      BucketMetadata bucket,
      UUID id,
      UUID uploadId,
      String partNumber,
      Path path,
      Map<String, String> encryptionHeaders) {
    var file = inputPathToFile(path, getPartPath(bucket, uploadId, partNumber));

    return hexDigest(encryptionHeaders.get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID), file);
  }

  public CompleteMultipartUploadResult completeMultipartUpload(
      BucketMetadata bucket,
      String key,
      UUID id,
      UUID uploadId,
      List<CompletedPart> parts,
      Map<String, String> encryptionHeaders,
      @Nullable MultipartUploadInfo uploadInfo,
      String location,
      @Nullable String checksum,
      @Nullable ChecksumAlgorithm checksumAlgorithm) {
    if (uploadInfo == null) {
      throw new IllegalArgumentException("Unknown upload " + uploadId);
    }
    var partFolder = getPartsFolder(bucket, uploadId);
    var partsPaths =
        parts
            .stream()
            .map(part ->
                Paths.get(partFolder.toString(), part.partNumber() + PART_SUFFIX)
            )
            .toList();
    Path tempFile = null;
    try {
      tempFile = Files.createTempFile("completeMultipartUpload", "");

      try (var is = toInputStream(partsPaths);
           var os = newOutputStream(tempFile)) {
        is.transferTo(os);
        var checksumFor = validateChecksums(uploadInfo, parts, partsPaths, checksum, checksumAlgorithm);
        var etag = hexDigestMultipart(partsPaths);
        var s3ObjectMetadata = objectStore.storeS3ObjectMetadata(bucket,
            id,
            key,
            uploadInfo.contentType(),
            uploadInfo.storeHeaders(),
            tempFile,
            uploadInfo.userMetadata(),
            encryptionHeaders,
            etag,
            uploadInfo.tags(),
            uploadInfo.checksumAlgorithm(),
            checksumFor,
            uploadInfo.upload().owner(),
            uploadInfo.storageClass(),
            ChecksumType.COMPOSITE
        );
        FileUtils.deleteDirectory(partFolder.toFile());
        return CompleteMultipartUploadResult.from(location,
            uploadInfo.bucket(),
            key,
            etag,
            uploadInfo,
            checksumFor,
            s3ObjectMetadata.checksumType(),
            checksumAlgorithm,
            s3ObjectMetadata.versionId()
        );
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format(
          "Error finishing multipart upload bucket=%s, key=%s, id=%s, uploadId=%s",
          bucket, key, id, uploadId), e);
    } finally {
      if (tempFile != null) {
        tempFile.toFile().delete();
      }
    }
  }

  @Nullable
  private String validateChecksums(
      MultipartUploadInfo uploadInfo,
      List<CompletedPart> completedParts,
      List<Path> partsPaths,
      @Nullable String checksum,
      @Nullable ChecksumAlgorithm checksumAlgorithm) {
    var checksumToValidate = checksum != null
        ? checksum
        : uploadInfo.checksum();
    var checksumAlgorithmToValidate = checksumAlgorithm != null
        ? checksumAlgorithm
        : uploadInfo.checksumAlgorithm();
    var checksumFor = checksumFor(partsPaths, uploadInfo);
    if (checksumAlgorithmToValidate != null) {
      for (var part : completedParts) {
        if (part.checksum(checksumAlgorithmToValidate) == null) {
          throw S3Exception.completeRequestMissingChecksum(
              checksumAlgorithmToValidate.toString().toLowerCase(),
              part.partNumber());
        }
      }
      if (checksumToValidate != null) {
        DigestUtil.verifyChecksum(checksumToValidate, checksumFor, checksumAlgorithmToValidate);
      }
    }

    return checksumFor;
  }

  @Nullable
  private String checksumFor(List<Path> paths, MultipartUploadInfo uploadInfo) {
    if (uploadInfo.checksumAlgorithm() != null) {
      return DigestUtil.checksumMultipart(paths,
          uploadInfo.checksumAlgorithm().toChecksumAlgorithm());
    }
    return null;
  }

  public List<Part> getMultipartUploadParts(
      BucketMetadata bucket,
      UUID id,
      UUID uploadId) {
    var partsPath = getPartsFolder(bucket, uploadId);
    try (var directoryStream = newDirectoryStream(partsPath,
            path -> path.getFileName().toString().endsWith(PART_SUFFIX))) {
      return StreamSupport.stream(directoryStream.spliterator(), false)
          .map(path -> {
            String name = path.getFileName().toString();
            String prefix = name.substring(0, name.indexOf('.'));
            int partNumber = Integer.parseInt(prefix);
            String partMd5 = hexDigest(path.toFile());
            Date lastModified = new Date(path.toFile().lastModified());

            return new Part(partNumber, partMd5, lastModified, path.toFile().length());
          })
          .sorted(Comparator.comparing(Part::partNumber))
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not read all parts. "
          + "bucket=%s, id=%s, uploadId=%s", bucket, id, uploadId), e);
    }
  }

  public String copyPart(
      BucketMetadata bucket,
      UUID id,
      @Nullable HttpRange copyRange,
      String partNumber,
      BucketMetadata destinationBucket,
      UUID destinationId,
      UUID uploadId,
      @Nullable Map<String, String> encryptionHeaders,
      @Nullable String versionId) {

    verifyMultipartUploadPreparation(destinationBucket, destinationId, uploadId);

    return copyPartToFile(bucket, id, copyRange,
        createPartFile(destinationBucket, destinationId, uploadId, partNumber), versionId);
  }

  private static InputStream toInputStream(List<Path> paths) {
    var result = new ArrayList<InputStream>();
    for (var path: paths) {
      try {
        result.add(Files.newInputStream(path));
      } catch (IOException e) {
        throw new IllegalStateException("Can't access path " + path, e);
      }
    }
    return new SequenceInputStream(Collections.enumeration(result));
  }

  private String copyPartToFile(
      BucketMetadata bucket,
      UUID id,
      @Nullable HttpRange copyRange,
      File partFile,
      @Nullable String versionId) {
    var from = 0L;
    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucket, id, versionId);
    var len = s3ObjectMetadata.dataPath().toFile().length();
    if (copyRange != null) {
      from = copyRange.getRangeStart(len);
      len = copyRange.getRangeEnd(len) - copyRange.getRangeStart(len) + 1;
    }

    try (var sourceStream = openInputStream(s3ObjectMetadata.dataPath().toFile());
        var targetStream = newOutputStream(partFile.toPath())) {
      var skip = sourceStream.skip(from);
      if (skip == from) {
        try (var bis = BoundedInputStream
            .builder()
            .setInputStream(sourceStream)
            .setMaxCount(len)
            .get()) {
          bis.transferTo(targetStream);
        }
      } else {
        throw new IllegalStateException("Could not skip exact byte range");
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not copy object. "
          + "bucket=%s, id=%s, range=%s, partFile=%s", bucket, id, copyRange, partFile), e);
    }
    return hexDigest(partFile);
  }

  @Nullable
  private File createPartFile(
      BucketMetadata bucket,
      @Nullable UUID id,
      UUID uploadId,
      String partNumber) {
    if (id == null) {
      return null;
    }
    var partFile = getPartPath(
        bucket,
        uploadId,
        partNumber).toFile();

    try {
      if (!partFile.exists() && !partFile.createNewFile()) {
        throw new IllegalStateException(String.format("Could not create buffer file. "
            + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber));
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not create buffer file. "
          + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber), e);
    }
    return partFile;
  }

  private static void validatePartNumber(String partNumber) {
    if (!partNumber.matches("^[1-9][0-9]*$")) {
      throw new IllegalArgumentException("Invalid part number: " + partNumber);
    }
  }

  private void verifyMultipartUploadPreparation(
      BucketMetadata bucket,
      @Nullable UUID id,
      UUID uploadId) {
    Path partsFolder = null;
    var multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId);
    if (id != null) {
      partsFolder = getPartsFolder(bucket, uploadId);
    }

    if (multipartUploadInfo == null
        || partsFolder == null
        || !partsFolder.toFile().exists()
        || !partsFolder.toFile().isDirectory()) {
      throw new IllegalStateException(String.format(
          "Multipart Request was not prepared. bucket=%s, id=%s, uploadId=%s, partsFolder=%s",
          bucket, id, uploadId, partsFolder));
    }
  }

  private boolean createPartsFolder(BucketMetadata bucket, UUID uploadId) {
    var partsFolder = getPartsFolder(bucket, uploadId).toFile();
    return partsFolder.mkdirs();
  }


  private Path getMultipartsFolder(BucketMetadata bucket) {
    return Paths.get(bucket.path().toString(), MULTIPARTS_FOLDER);
  }

  private Path getPartPath(BucketMetadata bucket, UUID uploadId, String partNumber) {
    validatePartNumber(partNumber);
    return getPartsFolder(bucket, uploadId).resolve(partNumber + PART_SUFFIX);
  }

  private Path getUploadMetadataPath(BucketMetadata bucket, UUID uploadId) {
    return getPartsFolder(bucket, uploadId).resolve(MULTIPART_UPLOAD_META_FILE);
  }

  private Path getPartsFolder(BucketMetadata bucket, UUID uploadId) {
    return getMultipartsFolder(bucket).resolve(uploadId.toString());
  }

  @Nullable
  private MultipartUploadInfo getUploadMetadata(BucketMetadata bucket, UUID uploadId) {
    var metaPath = getUploadMetadataPath(bucket, uploadId);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(uploadId)) {
        try {
          return objectMapper.readValue(metaPath.toFile(), MultipartUploadInfo.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read upload metadata-file " + uploadId, e);
        }
      }
    }
    return null;
  }

  private void writeMetafile(BucketMetadata bucket, MultipartUploadInfo uploadInfo) {
    var uploadId = uploadInfo.upload().uploadId();
    try {
      synchronized (lockStore.get(UUID.fromString(uploadId))) {
        var metaFile = getUploadMetadataPath(bucket, UUID.fromString(uploadId)).toFile();
        objectMapper.writeValue(metaFile, uploadInfo);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write upload metadata-file " + uploadId, e);
    }
  }
}
